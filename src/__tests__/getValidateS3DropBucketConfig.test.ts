import { getValidateS3DropBucketConfig } from '../handlers/getValidateS3DropBucketConfig'
import { s3, S3DB_Logging } from '../handlers/s3DropBucket'
import { GetObjectCommand } from '@aws-sdk/client-s3'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  s3: { send: jest.fn() }
}))

describe('getValidateS3DropBucketConfig', () => {
  const mockS3Send = s3.send as jest.MockedFunction<typeof s3.send>

  beforeEach(() => {
    jest.clearAllMocks()
    process.env.S3DropBucketConfigFile = 'test-config.json'
  })

  it('should get and validate config', async () => {
    const mockConfig = { s3dropbucket: 'test-bucket' }
    const mockBody = { transformToString: jest.fn().mockResolvedValue(JSON.stringify(mockConfig)) }
    const mockResult = { $metadata: { httpStatusCode: 200 }, Body: mockBody }
    mockS3Send.mockResolvedValue(mockResult)

    const result = await getValidateS3DropBucketConfig()

    expect(mockS3Send).toHaveBeenCalledWith(expect.any(GetObjectCommand))
    expect(result).toEqual(mockConfig)
  })

  it('should handle config errors', async () => {
    mockS3Send.mockRejectedValue(new Error('Config error'))

    await expect(getValidateS3DropBucketConfig())
      .rejects.toThrow('Config error')
  })
})