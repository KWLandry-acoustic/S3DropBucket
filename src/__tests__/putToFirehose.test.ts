import { putToFirehose } from '../handlers/putToFirehose'
import { fh_Client, S3DB_Logging } from '../handlers/s3DropBucket'
import { PutRecordCommand } from '@aws-sdk/client-firehose'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  fh_Client: { send: jest.fn() }
}))

describe('putToFirehose', () => {
  const mockFHSend = fh_Client.send as jest.MockedFunction<typeof fh_Client.send>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully put to Firehose', async () => {
    const mockResult = { $metadata: { httpStatusCode: 200 } }
    mockFHSend.mockResolvedValue(mockResult)

    const result = await putToFirehose([{ test: 'data' }], 'test-key', 'test-stream')

    expect(mockFHSend).toHaveBeenCalledWith(expect.any(PutRecordCommand))
    expect(result).toBe('200')
  })

  it('should handle Firehose errors', async () => {
    mockFHSend.mockRejectedValue(new Error('Firehose error'))

    await expect(putToFirehose([{ test: 'data' }], 'test-key', 'test-stream'))
      .rejects.toThrow('Firehose error')
  })
})