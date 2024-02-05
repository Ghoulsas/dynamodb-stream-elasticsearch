/* eslint-env mocha */
const { AWSElasticsearchClient } = require('../src/utils/aws-es-connection')
const chai = require('chai')
const { assert, expect } = chai
const spies = require('chai-spies')
chai.use(spies)
function withCallback (promise, callback) {
  promise
    .then((result) => callback(null, result))
    .catch((err) => callback(err, null))
}
describe('AWSElasticsearchClient', () => {
  let esClient
  let indexPrefix
  let spyCredentials

  before(async () => {
    const esEndpoint = 'http://localhost:4571'
    const awsEsClient = new AWSElasticsearchClient(esEndpoint)

    spyCredentials = chai.spy.on(awsEsClient.credentialsProvider, 'getCredentials')

    esClient = await awsEsClient.connect()

    indexPrefix = `aws-es-connection-tests-${new Date().getTime()}`
  })

  it('AWS creds are retrieved before each async call', async () => {
    await esClient.cat.health()
    expect(spyCredentials).to.have.been.called()
  })

  it('indices async', async () => {
    const indexName = indexPrefix + '-indices-async'
    try {
      // Create and retrieve index
      await esClient.indices.create({ index: indexName })
      const index = await esClient.indices.get({ index: indexName })
      assert.hasAnyKeys(index.body, indexName)
    } finally {
      // Delete index
      await esClient.indices.delete({ index: indexName })
    }
  })

  it('indices callback', (done) => {
    const indexName = indexPrefix + '-indices-callback'

    const cleanUp = (callback) => {
      withCallback(esClient.indices.delete({ index: indexName }), callback)
    }

    // Create and retrieve index
    withCallback(esClient.indices.create({ index: indexName }), (err) => {
      if (err) {
        return cleanUp(() => done(err))
      }

      withCallback(esClient.indices.get({ index: indexName }), (err, result) => {
        if (err) {
          return cleanUp(() => done(err))
        }

        try {
          assert.hasAnyKeys(result.body, indexName)
          cleanUp(done)
        } catch (error) {
          cleanUp(() => done(error))
        }
      })
    })
  })

  it('indexing and searching', async () => {
    const indexName = indexPrefix + '-searching'
    const doc1 = { name: 'John', body: 'Hello world' }
    const doc2 = { name: 'Joe', body: 'Lorem ipsum' }
    const doc3 = { name: 'Abbie', body: 'Hello, look at this' }

    try {
      // Create index and index some docs
      await esClient.indices.create({ index: indexName })
      await Promise.all([
        esClient.index({ index: indexName, refresh: 'wait_for', body: doc1 }),
        esClient.index({ index: indexName, refresh: 'wait_for', body: doc2 }),
        esClient.index({ index: indexName, refresh: 'wait_for', body: doc3 })
      ])

      const result = await esClient.search({ index: indexName, q: 'Hello' })
      assert.equal(result.body.hits.total.value, 2)
    } finally {
      // Clean up
      await esClient.indices.delete({ index: indexName })
    }
  }, 10000)
})
