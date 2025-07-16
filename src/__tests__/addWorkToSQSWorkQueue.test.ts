import { addWorkToSQSWorkQueue } from '../handlers/addWorkToSQSWorkQueue'
import { sqsClient, S3DB_Logging } from '../handlers/s3DropBucket'
import { SendMessageCommand } from '@aws-sdk/client-sqs'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  sqsClient: { send: jest.fn() }
}))

describe('addWorkToSQSWorkQueue', () => {
  const mockSQSSend = sqsClient.send as jest.MockedFunction<typeof sqsClient.send>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully add work to SQS queue', async () => {
    const mockResult = { $metadata: { httpStatusCode: 200 } }
    mockSQSSend.mockResolvedValue(mockResult)

    const result = await addWorkToSQSWorkQueue('test-key', 'test-version', {}, {}, 'test-queue')

    expect(mockSQSSend).toHaveBeenCalledWith(expect.any(SendMessageCommand))
    expect(result.SQSWriteResultStatus).toBe('200')
  })

  it('should handle SQS errors', async () => {
    mockSQSSend.mockRejectedValue(new Error('SQS error'))

    await expect(addWorkToSQSWorkQueue('test-key', 'test-version', {}, {}, 'test-queue'))
      .rejects.toThrow('SQS error')
  })
})