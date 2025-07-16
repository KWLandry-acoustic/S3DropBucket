import { addWorkToS3WorkBucket } from '../handlers/addWorkToS3WorkBucket'
import { s3, S3DB_Logging } from '../handlers/s3DropBucket'
import { PutObjectCommand } from '@aws-sdk/client-s3'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  s3: { send: jest.fn() }
}))

describe('addWorkToS3WorkBucket', () => {
  const mockS3Send = s3.send as jest.MockedFunction<typeof s3.send>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully add work to S3 bucket', async () => {
    const mockResult = { $metadata: { httpStatusCode: 200 }, VersionId: 'test-version' }
    mockS3Send.mockResolvedValue(mockResult)

    const result = await addWorkToS3WorkBucket('test-key', [], 'test-bucket')

    expect(mockS3Send).toHaveBeenCalledWith(expect.any(PutObjectCommand))
    expect(result.S3ProcessBucketResultStatus).toBe('200')
  })

  it('should handle S3 errors', async () => {
    mockS3Send.mockRejectedValue(new Error('S3 error'))

    await expect(addWorkToS3WorkBucket('test-key', [], 'test-bucket'))
      .rejects.toThrow('S3 error')
  })
})