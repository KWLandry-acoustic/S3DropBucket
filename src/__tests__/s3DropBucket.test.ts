import {S3DB_Logging, s3} from '../handlers/s3DropBucket'

jest.mock('../handlers/s3DropBucket', () => ({
  ...jest.requireActual('../handlers/s3DropBucket'),
  s3: {
    config: {
      region: jest.fn().mockResolvedValue('us-east-1')
    }
  }
}))

describe('S3DB_Logging', () => {
      let mockConsoleInfo: jest.SpyInstance
      let mockConsoleWarn: jest.SpyInstance
      let mockConsoleError: jest.SpyInstance
      let mockConsoleDebug: jest.SpyInstance

      beforeEach(() => {
            mockConsoleInfo = jest.spyOn(console, 'info').mockImplementation()
            mockConsoleWarn = jest.spyOn(console, 'warn').mockImplementation()
            mockConsoleError = jest.spyOn(console, 'error').mockImplementation()
            mockConsoleDebug = jest.spyOn(console, 'debug').mockImplementation()

            process.env.S3DropBucketLogLevel = 'ALL'
            process.env.S3DropBucketSelectiveLogging = '_97,_98,_99,'
      })

      afterEach(() => {
            mockConsoleInfo.mockRestore()
            mockConsoleWarn.mockRestore()
            mockConsoleError.mockRestore()
            mockConsoleDebug.mockRestore()

            delete process.env.S3DropBucketLogLevel
            delete process.env.S3DropBucketSelectiveLogging
      })

      test('Should log messages with different levels', async () => {
            await S3DB_Logging('info', '97', 'Info message')
            await S3DB_Logging('warn', '98', 'Warning message')
            await S3DB_Logging('error', '99', 'Error message')
            await S3DB_Logging('debug', '100', 'Debug message')
            await S3DB_Logging('exception', '', 'Exception message')

            expect(mockConsoleInfo).toHaveBeenCalledWith(expect.stringContaining('S3DBLog-Info (LOG ALL-97): Info message'))
            expect(mockConsoleWarn).toHaveBeenCalledWith(expect.stringContaining('S3DBLog-Warning (LOG ALL-98): Warning message'))
            expect(mockConsoleError).toHaveBeenCalledWith(expect.stringContaining('S3DBLog-Error (LOG ALL-99): Error message'))
            expect(mockConsoleDebug).toHaveBeenCalledWith(expect.stringContaining('S3DBLog-Debug (LOG ALL-100): Debug message'))
            expect(mockConsoleError).toHaveBeenCalledWith(expect.stringContaining('S3DBLog-Exception : Exception message'))
      })
})
