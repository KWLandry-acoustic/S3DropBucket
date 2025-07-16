import { getAccessToken } from '../handlers/getAccessToken'
import { S3DB_Logging } from '../handlers/s3DropBucket'

jest.mock('../handlers/s3DropBucket', () => ({
  S3DB_Logging: jest.fn()
}))

global.fetch = jest.fn()

describe('getAccessToken', () => {
  const mockFetch = fetch as jest.MockedFunction<typeof fetch>

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('should successfully get access token', async () => {
    const mockResponse = {
      ok: true,
      json: jest.fn().mockResolvedValue({
        access_token: 'test-token',
        token_type: 'Bearer',
        expires_in: 3600
      })
    }
    mockFetch.mockResolvedValue(mockResponse as any)

    const result = await getAccessToken('client-id', 'client-secret', 'refresh-token', 'auth-url')

    expect(result.access_token).toBe('test-token')
  })

  it('should handle auth errors', async () => {
    mockFetch.mockRejectedValue(new Error('Auth error'))

    await expect(getAccessToken('client-id', 'client-secret', 'refresh-token', 'auth-url'))
      .rejects.toThrow('Auth error')
  })
})