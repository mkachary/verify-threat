import logging
from Crypto.Cipher import AES
from base64 import b64encode, b64decode
from os import makedirs, mkdir, urandom
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)

log_dir='data/logs'
if( not os.path.exists(log_dir)):
    os.makedirs(log_dir)

file_handler = logging.FileHandler('data/logs/app.log')
logger.addHandler(file_handler)


def pad(s):
    return s + bytes([16-(len(s) % 16)])*(16-(len(s) % 16))


def unpad(s):
    return s[:-s[-1]]


class Encryption:
    def __init__(self, key: str):
        """
        Initialize an Encryption object with a key
        :param key: Any string of length <= 32.
        """
        # AES key length can 16Bytes, 24Bytes, 32Bytes long.
        # Choose a key of length <= 32 Bytes
        key = pad(key.encode('utf8'))
        if len(key) > 32:
            logger.exception('Check Key Length (Length should be <= 32)')
            raise Exception('Check Key Length (Length should be <= 32)')
        self.key = key

    def encrypt(self, plaintext: str):
        """
        Encrypt a plaintext using the key and AES-CBC Encryption technique
        :param plaintext: String that you want to encrypt.
        :return: ciphertext: Encrypted text (bytes)
        """
        plaintext = pad(str(plaintext).encode('utf8'))
        iv = urandom(16)
        ciphertext = b64encode(iv + AES.new(self.key, AES.MODE_CBC, iv).encrypt(plaintext))
        return ciphertext

    def decrypt(self, ciphertext: bytes):
        """
        Decrypt the ciphertext using the key.
        :param ciphertext: Byte encoded encrypted form of the string
        :return: plaintext: String which is the decrypted ciphertext
        """
        ciphertext = b64decode(ciphertext)
        iv = ciphertext[:16]
        enc = ciphertext[16:]
        dec = AES.new(self.key, AES.MODE_CBC, iv).decrypt(enc)
        received = unpad(dec).decode('utf8')
        return received


# random_text = ['Krishnakant', 'Amit', 'Rahul']
# enc = Encryption(key = 'encrypt')
# print(bytes([5]))
# print([pad(x.encode('utf8')) for x in random_text])
# padded = [pad(x.encode('utf8')) for x in random_text]
# print([unpad(x) for x in padded])
# print([enc.encrypt(x) for x in random_text])
# encript = [enc.encrypt(x) for x in random_text]
# print([enc.decrypt(x) for x in encript])