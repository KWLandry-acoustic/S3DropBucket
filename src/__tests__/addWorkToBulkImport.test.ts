import { addWorkToBulkImport } from '../handlers/addWorkToBulkImport'
import { S3DB_Logging, s3 } from '../handlers/s3DropBucket'
import { PutObjectCommand } from '@aws-sdk/client-s3'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn(),
  s3: {
    send: jest.fn()
  }
}))

jest.mock('@aws-sdk/client-s3')

describe('addWorkToBulkImport', () => {
  const mockS3Send = s3.send as jest.MockedFunction<typeof s3.send>
  const mockS3DBLogging = S3DB_Logging as jest.MockedFunction<typeof S3DB_Logging>

  const mockS3DBConfig = {
    s3dropbucket_bulkimport: 'test-bulk-import-bucket',
    s3dropbucket_bulkimportquiesce: false
  }

  const mockCustConfig = {
    subscriptionid: 'test-subscription'
  }

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should return quiesce status when bulk import is quiesced', async () => {
    const quiescedConfig = { ...mockS3DBConfig, s3dropbucket_bulkimportquiesce: true }
    
    const result = await addWorkToBulkImport('test-key', [], quiescedConfig, mockCustConfig)
    
    expect(result).toEqual({
      BulkImportWriteResultStatus: '',
      AddWorkToBulkImportResult: 'In Quiesce'
    })
    expect(mockS3DBLogging).toHaveBeenCalledWith('warn', '526', expect.stringContaining('BulkImport Quiesce is in effect'))
  })

  it('should successfully write to bulk import bucket', async () => {
    const mockPutResult = { $metadata: { httpStatusCode: 200 } }
    mockS3Send.mockResolvedValue(mockPutResult)

    const result = await addWorkToBulkImport('test-key', [{ data: 'test' }], mockS3DBConfig, mockCustConfig)

    expect(mockS3Send).toHaveBeenCalledWith(expect.any(PutObjectCommand))
    expect(result.BulkImportWriteResultStatus).toBe('"200"')
    expect(mockS3DBLogging).toHaveBeenCalledWith('info', '527', expect.stringContaining('Added Updates'))
  })

  it('should throw error when S3 put fails', async () => {
    const mockError = new Error('S3 put failed')
    mockS3Send.mockRejectedValue(mockError)

    await expect(addWorkToBulkImport('test-key', [], mockS3DBConfig, mockCustConfig))
      .rejects.toThrow('Exception - Put Object Command for writing work')
    
    expect(mockS3DBLogging).toHaveBeenCalledWith('exception', '', expect.stringContaining('Exception - Put Object Command'))
  })
})