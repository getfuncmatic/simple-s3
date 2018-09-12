var AWS = require('aws-sdk')
const mime = require('mime-types')
const { promisify } = require('util')
const isStream = require('is-stream')
const createS3Stream = require('s3-upload-stream')

const TEXT_MIMETYPES = [
  'application/json',
  'text/plain'
]

const DEFAULT_BINARY_MIMETYPE = "application/octet-stream"
const DEFAULT_TEXT_MIMETYPE = "text/plain"

class S3 {
  constructor(config) {
    this.accessKeyId = config.accessKeyId
    this.secretAccessKey = config.secretAccessKey
    this.region = config.region
    this.bucket = config.bucket
    this.s3 = new AWS.S3({
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: config.region
    })
    this.createPromisifiedMethods(this.s3)
  }

  createPromisifiedMethods(s3) {
    this.getObject = promisify(s3.getObject).bind(s3)
    this.putObject = promisify(s3.putObject).bind(s3)
    this.listObjectsV2 = promisify(s3.listObjectsV2).bind(s3)
    this.deleteObjects = promisify(s3.deleteObjects).bind(s3)
  }

  async get(key, options) {
    options = options || { }
    var mimetype = options.ContentType || mime.lookup(key) || DEFAULT_BINARY_MIMETYPE
    var params = {
      Bucket: options.bucket || this.bucket,
      Key: key,
      ResponseContentType: mimetype
    }
    if (options.stream) {
      return this.readable(key, params)
    }
    var data = await this.getObject(params)
    var body = data.Body
    if (isTextType(mimetype)) {
      body = body.toString('utf-8')
    }
    if (mimetype == "application/json") {
      body = JSON.parse(body)
    }
    return body
  }

  readable(key, params) {
    return this.s3.getObject(params).createReadStream()
  }
    
  async put(key, body, options) {
    options = options || { }
    var params = {
      Bucket: options.bucket || this.bucket,
      Key: key
    }
    if (options.ContentType) {
      params.ContentType = options.ContentType
    } else if (mime.lookup(key)) {
      params.ContentType = mime.lookup(key)
    } else if (typeof body == 'string') {
      params.ContentType = DEFAULT_TEXT_MIMETYPE
    } else {
      params.ContentType = DEFAULT_BINARY_MIMETYPE
    }
    if (isStream(body)) {
      return await this.putStream(key, body, params, options)
    } else {
      params.Body = body
      var data = await this.putObject(params)
      return data
    }
  }

  async putStream(key, readable, params, options) {
    var s3Stream = createS3Stream(this.s3)
    var upload = s3Stream.upload(params)
    if (options.maxPartSize) upload.maxPartSize = options.maxPartSize
    if (options.concurrentParts) upload.concurrentParts = options.concurrentParts
    var ret = new Promise((resolve, reject) => {
      upload.on('error', (err) => {
        reject(err)
      })
      upload.on('uploaded', (data) => {
        resolve(data)
      })
    })
    readable.pipe(upload)
    return ret
  }

  async delete(keys, options) {
    options = options || { }
    if (typeof keys == 'string') {
      keys = [ keys ]
    }
    var objects = [ ]
    for (var i=0; i<keys.length; i++) {
      objects.push({
        Key: keys[i]
      })
    }
    var params = {
      Bucket: options.bucket || this.bucket,
      Delete: {
        Objects: objects
      }
    }
    var data = await this.deleteObjects(params)
    var keys = [ ]
    for (var i=0; i<data.Deleted.length; i++) {
      keys.push(data.Deleted[i].Key)
    }
    data.keys = keys
    return data
  }

  async list(prefix, options) {
    options = options || { }
    var params = {
      Bucket: options.bucket || this.bucket
    }
    if (prefix) {
      params.Prefix = prefix
    }
    var data = await this.listObjectsV2(params)
    var keys = [ ]
    for (var i=0; i<data.Contents.length; i++) {
      keys.push(data.Contents[i].Key)  
    }
    data.keys = keys
    return data
  }
}

function isTextType(mimetype) {
  return (TEXT_MIMETYPES.indexOf(mimetype) >= 0)
}

S3.createInstance = function(config) {
  return new S3(config)
}

module.exports = S3
