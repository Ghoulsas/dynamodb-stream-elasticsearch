const { Client, Connection, Transport } = require('@elastic/elasticsearch')
const { defaultProvider } = require('@aws-sdk/credential-provider-node')
const { SignatureV4 } = require('@smithy/signature-v4')
const { HttpRequest } = require('@smithy/protocol-http')
const { NodeHttpHandler } = require('@smithy/node-http-handler')
const { Sha256 } = require('@aws-crypto/sha256-browser')

class AWSCredentialsProvider {
  async getCredentials () {
    return await defaultProvider()()
  }
}

class AWSSignedConnection extends Connection {
  constructor (opts, awsCredentials) {
    super(opts)
    this.awsCredentials = awsCredentials
  }

  async signedRequest (reqParams) {
    const request = new HttpRequest({
      ...reqParams,
      protocol: 'https:',
      hostname: reqParams.host,
      port: 443,
      path: reqParams.path,
      method: reqParams.method,
      headers: reqParams.headers,
      body: reqParams.body
    })

    const signer = new SignatureV4({
      credentials: this.awsCredentials,
      service: 'es',
      sha256: Sha256
    })

    const signedRequest = await signer.sign(request)
    const httpHandler = new NodeHttpHandler()
    const { response } = await httpHandler.handle(signedRequest)
    return response
  }
}

class AWSElasticsearchClient {
  constructor (node, options = {}) {
    this.node = { node }
    this.options = options
    this.credentialsProvider = options?.provider === 'aws' ? new AWSCredentialsProvider() : null
  }

  async connect () {
    if (!this.credentialsProvider) {
      return new Client({
        ...this.node,
        ...this.options
      })
    }

    const awsCredentials = await this.credentialsProvider.getCredentials()

    const AWSConnection = {
      Connection: class extends AWSSignedConnection {
        constructor (opts) {
          super(opts, awsCredentials)
        }
      },
      Transport: class extends Transport {
        async request (params, options) {
          return super.request(params, options)
        }
      }
    }

    return new Client({
      ...AWSConnection,
      ...this.node,
      ...this.options
    })
  }
}

module.exports = { AWSElasticsearchClient, AWSCredentialsProvider, AWSSignedConnection }
