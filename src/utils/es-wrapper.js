const { AWSElasticsearchClient } = require('./aws-es-connection')

module.exports = async (node, options) => {
  const awsEsClient = new AWSElasticsearchClient(node, options)

  const es = await awsEsClient.connect()
  return {
    index: ({ index, id, body, refresh }) => es.index({ index, id, body, refresh, timeout: '5m' }),
    remove: ({ index, id, refresh }) => es.delete({ index, id, refresh }),
    exists: ({ index, id, refresh }) => es.exists({ index, id, refresh }),
    indicesDelete: (index = '_all') => es.indices.delete({ index }),
    bulk: ({ refresh = true, body }) => es.bulk({ refresh, body })
  }
}
