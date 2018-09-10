require('dotenv').config()

const fs = require('fs')
const path = require('path')
var AWS = require('aws-sdk')
const S3 = require('../lib/s3')
const util = require('util')

const TESTBUCKET = "test.funcmatic.com"
const TESTKEY = "s3-plugin-funcmatic/test.json"
const TESTMIMETYPE = "application/json"

const TESTBINKEY = 's3-plugin-funcmatic/image.test.jpg'
const TESTBINFILE = path.join(__dirname, 'image.test.jpg')
const TESTSTREAMBINKEY = 's3-plugin-funcmatic/image.stream.jpg'
const TESTSTREAMBINFILE = path.join(__dirname, 'image.stream.jpg')

describe('Basic S3 Operations', () => {
  var s3 = null
  beforeEach(async () => {
    s3 = S3.createInstance({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
      bucket: TESTBUCKET
    })
  })
  it ('should put, get, list, and delete a JSON object', async () => {
    var jsonstr = JSON.stringify({ hello: "world" })
    var res = await s3.put(TESTKEY, jsonstr)
    expect(res).toMatchObject({
      ETag: expect.anything()
    })
    var data = await s3.get(TESTKEY)
    expect(data).toMatchObject({
      hello: "world"
    })
    var list = await s3.list(TESTKEY.split("/")[0])
    expect(list.keys).toContainEqual(TESTKEY)
    var del = await s3.delete(TESTKEY)
    expect(del).toMatchObject({
      Errors: [ ],
      keys: [ TESTKEY ]
    })
  })
  it ('should put and get a binary file (image)', async () => {
    var img = fs.readFileSync(TESTBINFILE)
    var res = await s3.put(TESTBINKEY, img)
    expect(res).toMatchObject({
      ETag: expect.anything()
    })
    var data = await s3.get(TESTBINKEY)
    expect(util.isBuffer(data)).toBe(true)
  })
  it ('should put a readable stream using streaming', async () => {
    var readable = fs.createReadStream(TESTSTREAMBINFILE)
    var res = await s3.put(TESTSTREAMBINKEY, readable)
    expect(res).toMatchObject({
      ETag: expect.anything()
    })

    var download = await s3.get(TESTSTREAMBINKEY, { stream: true }) 
    download.pipe(fs.createWriteStream(path.join(__dirname, 'download.test.jpg')))

  }, 5 * 60 * 1000)
})


