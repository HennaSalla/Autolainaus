# CIPHER.PY -MODUULIN YKSIKKÖTESTIT
# =================================

import pytest # Järjestelmätason virheiden testaus
import modules.cipher as cipher # Testattavan moduulin lataus

plainText = b'Selkokieliteksti'
key = b'oqjptt7iNxAnpF4DnKyVmcSv9mu3feeVChxBHZijMsI='
chipherEngine = cipher.createChipher(key)
cryptoText = cipher.encrypt(chipherEngine, plainText)

def test_decrypt():
    assert cipher.decrypt(chipherEngine, cryptoText, True) == plainText 

# Luodaan salateksti käyttämällä engryptString-funkitota
cryptoText2 = cipher.encryptString('Selkokieliteksti')

# Tehdään testi, joka käyttää decryptString-funktiota
def test_decryptString():
    assert cipher.decryptString(cryptoText2) == 'Selkokieliteksti'
