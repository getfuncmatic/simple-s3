var AWS = require('aws-sdk')
const mime = require('mime-types')
const { promisify } = require('util')
const isStream = require('is-stream')
const createS3Stream = require('s3-upload-stream')

const TEXT_MIMETYPES = [
  'application/json',
  'text/plain'
]
// var client = redis.createClient(options.uri, { password: options.password })
// var get = promisify(client.get).bind(client)
// var set = promisify(client.set).bind(client)
// var del = promisify(client.del).bind(client)

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
    console.log("GET DATA", data)
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
      params.ContentType = DEFAULT_TEXT_CONTENTYPE
    } else {
      params.ContentType = DEFAULT_BINARY_CONTENTYPE
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

  // s3Params(options) {
  //   var params = {
  //     Bucket: options.Bucket || this.Bucket
  //   }
  //   return params
  // }
  

  //   return new Promise((resolve, reject) => {
  //     var params = {
  //       Bucket: options.Bucket || this.Bucket,
  //       Key: key
  //     }
  //     this.s3.getObject(params, function(err, data) {
  //       if (err) {
  //         reject(err)
  //         return
  //       }
  //       resolve(data.Body.toString('utf-8'))
  //       return
  //     })
  //   })
  // }


  
  // /*
  //   [ { Key: 'test.json' } ], Errors: [ ] }
  // */
  // delete(keys, options) {
  //   options = options || { }
  //   if (typeof keys == 'string') {
  //     keys = [ keys ]
  //   }
  //   var objects = [ ]
  //   for (var i=0; i<keys.length; i++) {
  //     objects.push({
  //       Key: keys[i]
  //     })
  //   }
  //   return new Promise((resolve, reject) => {
  //     var params = {
  //       Bucket: options.Bucket || this.Bucket,
  //       Delete: {
  //         Objects: objects
  //       }
  //     }
  //     this.s3.deleteObjects(params, function(err, data) {
  //       if (err) {
  //         reject(err)
  //         return
  //       }
  //       var keys = [ ]
  //       for (var i=0; i<data.Deleted.length; i++) {
  //         keys.push(data.Deleted[i].Key)
  //       }
  //       data.keys = keys
  //       resolve(data)
  //       return
  //     })
  //   }) 
  // }
  
  // /*
  // data = {
  // Contents: [
  //    {
  //   ETag: "\"70ee1738b6b21e2c8a43f3a5ab0eee71\"", 
  //   Key: "happyface.jpg", 
  //   LastModified: <Date Representation>, 
  //   Size: 11, 
  //   StorageClass: "STANDARD"
  //  }, 
  //    {
  //   ETag: "\"becf17f89c30367a9a44495d62ed521a-1\"", 
  //   Key: "test.jpg", 
  //   LastModified: <Date Representation>, 
  //   Size: 4192256, 
  //   StorageClass: "STANDARD"
  //  }
  // ], 
  // IsTruncated: true, 
  // KeyCount: 2, 
  // MaxKeys: 2, 
  // Name: "examplebucket", 
  // NextContinuationToken: "1w41l63U0xa8q7smH50vCxyTQqdxo69O3EmK28Bi5PcROI4wI/EyIJg==", 
  // Prefix: ""
  // }
  // */
  // list(prefix, options) {
  //   options = options || { }
  //   return new Promise((resolve, reject) => {
  //     var params = {
  //       Bucket: options.Bucket || this.Bucket
  //     }
  //     if (prefix) {
  //       params.Prefix = prefix
  //     }
  //     this.s3.listObjectsV2(params, function(err, data) {
  //       if (err) {
  //         reject(err)
  //         return
  //       }
  //       var keys = [ ]
  //       for (var i=0; i<data.Contents.length; i++) {
  //         keys.push(data.Contents[i].Key)  
  //       }
  //       data.keys = keys
  //       resolve(data)
  //       return
  //     })
  //   })
  // }
  
  // /*
  //  { CopyObjectResult: 
  //      { ETag: '"fbc24bcc7a1794758fc1327fcfebdaf6"',
  //        LastModified: Thu Dec 07 2017 16:02:12 GMT+0000 (UTC) } }
  // */
  // copy(srckey, dstkey, options) {
  //   options = options || { }
  //   var SrcBucket = options.SrcBucket || this.Bucket
  //   return new Promise((resolve, reject) => {
  //     var params = {
  //       Bucket: options.Bucket || this.Bucket,
  //       CopySource: `/${SrcBucket}/${srckey}`,
  //       Key: dstkey
  //     }
  //     this.s3.copyObject(params, function(err, data) {
  //         if (err) {
  //           reject(err)
  //           return
  //         }
  //         resolve(data)
  //         return
  //       })
  //     })
  //   }
  // }
}

function isTextType(mimetype) {
  return (TEXT_MIMETYPES.indexOf(mimetype) >= 0)
}

S3.createInstance = function(config) {
  return new S3(config)
}

module.exports = S3
