# MODUULI SALAUSAVAINTEN JA FRENET-SALAUKSEEN JA SEN PURKAMISEEN
# ==============================================================

# KIRJASTOJEN JA MODUULIEN LATAUKSET
from cryptography.fernet import Fernet

def newKey() -> str:
    """Creates a new key for encrypting and decrypting messages

    Returns:
        str: a key in byte form
    """
    key = Fernet.generate_key()
    return key

def createChipher(key: str) -> object:
    """Creates a new chipher ie. encrypting machine

    Args:
        key (str): A fernet generated key

    Returns:
        object: The cipher objet to use to encrypt or decrypt
    """
    chipher = Fernet(key)
    return chipher

def encrypt(chipher, plainText):
    """Encrypts a message usinf Fernet algorithm

    Args:
        chipher (object): Fernet chiphering engine
        plainText (str): Text to be encrypted

    Returns:
        str: encrypted text in byte format
    """
    cryptoText = chipher.encrypt(plainText)
    return cryptoText

def decrypt(chipher, cryptoText, byteMode=False):
    """Decrypts a message

    Args:
        chipher (object): Decrypting engine
        cryptoText (str): Encrypted text to be decrypted
        byteMode (bool, optional): If return value will be in byte form. Defaults to False.

    Returns:
        str: message in plain text
    """
    if byteMode == True:
        plaintext = chipher.decrypt(cryptoText)
    else:
        plaintext = chipher.decrypt(cryptoText).decode()
    return plaintext

# TODO: Lisää jossain vaiheessa funktiot, jotka ottavat parametriksi vain avaimen ja tekstin

if __name__ == "__main__":
    secretKey = newKey()
    print(secretKey)
