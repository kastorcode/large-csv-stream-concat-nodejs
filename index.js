import csvtojson from 'csvtojson'
import debug from 'debug'
import { createReadStream, createWriteStream, existsSync, promises } from 'fs'
import jsontocsv from 'json-to-csv-stream'
import { dirname, join } from 'path'
import { pipeline, Transform } from 'stream'
import StreamConcat from 'stream-concat'
import { promisify } from 'util'


const log = debug('app:concat')

const { pathname } = new URL(import.meta.url)
const __dirname = dirname(pathname)
const dataset = `${__dirname}/dataset`
const output = `${dataset}/output.csv`

const { readdir, rm } = promises
existsSync(output) && await rm(output)
const files = await readdir(dataset)
const pipelineAsync = promisify(pipeline)


console.time('concat-data')
log(`processing ${files}`)
setInterval(() => process.stdout.write('.'), 1000).unref()


const streams = files.map(file => createReadStream(join(dataset, file)))
const readStreams = new StreamConcat(streams)
const writeStream = createWriteStream(output)
const handleStream = new Transform({
  transform: (chunk, encoding, callback) => {
    const parsed = JSON.parse(chunk)
    const data = {
      id: parsed.Respondent, country: parsed.Country
    }
    return callback(null, JSON.stringify(data))
  }
})
await pipelineAsync(readStreams, csvtojson(), handleStream, jsontocsv(), writeStream)


log(`\n${files.length} files merged on ${output}`)
console.timeEnd('concat-data')