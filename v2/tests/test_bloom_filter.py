from v2.filter import BloomFilter


def test_bloom():
    # auto reset every 100 insertions, so test should not failed even we inset 2000 numbers
    bf = BloomFilter(insertions=100, fpp=0.01)
    for i in range(2000):
        exist = bf.put(i)
        assert exist == False
    bf.reset()
    for i in range(40):
        exist = bf.put(i)
        assert exist == False
    # test exists
    for i in range(40):
        exist = bf.put(i)
        assert exist == True
