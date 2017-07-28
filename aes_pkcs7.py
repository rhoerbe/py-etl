#!/usr/bin/python3
from binascii      import hexlify, unhexlify
from Crypto.Cipher import AES
from Crypto        import Random

def pad (s, blocksize = 16) :
    """ Pad s to blocksize, if len (s) is an integer multiple of
        blocksize we add a full block of padding
    >>> pad (b'1234', 64)
    b'1234<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
    >>> pad (b'1234567890', 64)
    b'1234567890666666666666666666666666666666666666666666666666666666'
    >>> pad (b'', 64)
    b'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'
    """
    pad_len = blocksize - (len (s) % blocksize)
    if pad_len == 0 :
        pad_len = blocksize
    return s + bytes (range (pad_len, pad_len + 1)) * pad_len
# end def pad

def unpad (s) :
    """ Unpadding doesn't need the blocksize because padding is
        determined from the info in the padding itself.
    >>> unpad (b'1234' + bytes (range (4, 5)) * 4)
    b'1234'
    >>> unpad (b'1234567890' + b'6' * 54)
    b'1234567890'
    >>> unpad (b'@' * 64)
    b''
    """
    pad_len = s [-1]
    return s [:len (s) - pad_len]
# end def unpad

class AES_Cipher (object) :
    """ Encrypt with pkcs7 RFC 5652 padding with 128 bit (16 byte) blocksize
        See https://en.wikipedia.org/wiki/Padding_(cryptography)#PKCS7
        Some code stolen from https://gist.github.com/crmccreary/5610068
        (which is originally for pkcs5 which is essentially pkcs7 with
        a fixed blocksize of 64) but the padding code was really
        obfuscated and is essential. See above for an added doctest.
    """

    def __init__ (self, hexkey) :
        self.key = unhexlify (hexkey)
    # end def __init__

    def encrypt (self, raw, iv = None) :
        """ Returns hex encoded encrypted value!
            Note that passing in the iv is usually only done for
            regression testing with a known iv!
        """
        raw = pad (raw, AES.block_size)
        if iv is None :
            iv = Random.new ().read (AES.block_size)
        cipher = AES.new (self.key, AES.MODE_CBC, iv)
        return hexlify (iv + cipher.encrypt (raw))
    # end def encrypt

    def decrypt (self, enc) :
        """ Requires hex encoded param to decrypt
        """
        enc = unhexlify (enc)
        iv  = enc [:16]
        enc = enc [16:]
        cipher = AES.new (self.key, AES.MODE_CBC, iv)
        return unpad (cipher.decrypt (enc))
    # end def decrypt

# end class AES_Cipher

def main () :
    """
    >>> main ()
    """
    key   = b'01010101010101010101010101010101'
    plain = 'This is just a test'.encode ('utf-8')
    aes   = AES_Cipher (key)
    ciph  = aes.encrypt (plain)
    assert plain == aes.decrypt (ciph)
# end def main

if __name__ == '__main__' :
    main ()
