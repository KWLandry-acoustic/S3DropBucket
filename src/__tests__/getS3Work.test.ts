import { getS3Work } from '../handlers/getS3Work'
import { s3, S3DB_Logging } from '../handlers/s3DropBucket'
import { GetObjectCommand } from '@aws-sdk/client-s3'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  s3: { send: jest.fn() }
}))

describe('getS3Work', () => {
  const mockS3Send = s3.send as jest.MockedFunction<typeof s3.send>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully get S3 work', async () => {
    const mockBody = { transformToString: jest.fn().mockResolvedValue('{"test": "data"}') }
    const mockResult = { $metadata: { httpStatusCode: 200 }, Body: mockBody }
    mockS3Send.mockResolvedValue(mockResult)

    const result = await getS3Work('test-key', 'test-bucket')

    expect(mockS3Send).toHaveBeenCalledWith(expect.any(GetObjectCommand))
    expect(result).toEqual([{ test: 'data' }])
  })

  it('should handle get work errors', async () => {
    mockS3Send.mockRejectedValue(new Error('Get work error'))

    await expect(getS3Work('test-key', 'test-bucket'))
      .rejects.toThrow('Get work error')
  })
})