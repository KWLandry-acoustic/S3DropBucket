import { applyJSONMap } from '../handlers/applyJSONMap'
import { S3DB_Logging } from '../handlers/s3DropBucket'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn()
}))

describe('applyJSONMap', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should apply JSON mapping', async () => {
    const testData = { oldField: 'value' }
    const jsonMap = { newField: 'oldField' }

    const result = await applyJSONMap(testData, jsonMap)

    expect(result).toHaveProperty('newField', 'value')
  })

  it('should handle empty mapping', async () => {
    const testData = { field: 'value' }
    const jsonMap = {}

    const result = await applyJSONMap(testData, jsonMap)

    expect(result).toEqual(testData)
  })
})