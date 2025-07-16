import { getFormatCustomerConfig } from '../handlers/getFormatCustomerConfig'
import { S3DB_Logging } from '../handlers/s3DropBucket'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn()
}))

jest.mock('../handlers/getAllCustomerConfigsList', () => ({
  getAllCustomerConfigsList: jest.fn().mockResolvedValue([
    { customer: 'test-customer', format: 'json' }
  ])
}))

describe('getFormatCustomerConfig', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should get customer config by key', async () => {
    const result = await getFormatCustomerConfig('test-customer_file.json')

    expect(result).toHaveProperty('customer', 'test-customer')
  })

  it('should handle missing config', async () => {
    await expect(getFormatCustomerConfig('unknown_file.json'))
      .rejects.toThrow()
  })
})