import unittest

testsuite = unittest.TestLoader().discover('.')
unittest.TextTestRunner().run(testsuite)
