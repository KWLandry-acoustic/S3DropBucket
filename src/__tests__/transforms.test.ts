import { applyTransforms } from '../handlers/transforms'
import { S3DB_Logging } from '../handlers/s3DropBucket'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn()
}))

describe('transforms', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should apply transforms to data', async () => {
    const testData = { date: '2024-01-01', phone: '1234567890' }
    const transforms = {
      methods: {
        date_iso1806_type: { date: 'iso' },
        phone_number_type: { phone: 'format' }
      }
    }

    const result = await applyTransforms(testData, transforms)

    expect(result).toBeDefined()
  })

  it('should handle empty transforms', async () => {
    const testData = { field: 'value' }
    const transforms = { methods: {} }

    const result = await applyTransforms(testData, transforms)

    expect(result).toEqual(testData)
  })
})