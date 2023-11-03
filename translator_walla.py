from deep_translator import GoogleTranslator

class Translator:
    def __init__(self):
        translator = GoogleTranslator(source='auto', target='en')
        self.translator = translator

    def translate(self, sentence):
        translated = self.translator.translate(sentence)
        return translated
    
# Example
translator = Translator()
sentence = "שלום עולם"
translated = translator.translate(sentence)