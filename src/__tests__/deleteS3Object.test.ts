import { deleteS3Object } from '../handlers/deleteS3Object'
import { s3, S3DB_Logging } from '../handlers/s3DropBucket'
import { DeleteObjectCommand } from '@aws-sdk/client-s3'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  s3: { send: jest.fn() }
}))

describe('deleteS3Object', () => {
  const mockS3Send = s3.send as jest.MockedFunction<typeof s3.send>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully delete S3 object', async () => {
    const mockResult = { $metadata: { httpStatusCode: 204 } }
    mockS3Send.mockResolvedValue(mockResult)

    const result = await deleteS3Object('test-key', 'test-bucket')

    expect(mockS3Send).toHaveBeenCalledWith(expect.any(DeleteObjectCommand))
    expect(result).toBe('204')
  })

  it('should handle delete errors', async () => {
    mockS3Send.mockRejectedValue(new Error('Delete error'))

    await expect(deleteS3Object('test-key', 'test-bucket'))
      .rejects.toThrow('Delete error')
  })
})