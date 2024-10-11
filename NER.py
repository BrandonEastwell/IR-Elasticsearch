import spacy
nlp = spacy.load("en_core_web_sm")

doc = nlp( "Of the various science fiction awards, the Saturn Awards is the most frequent award for the Marvel Cinematic Universe; in 13 years, MCU films have been nominated for 159 Saturn Awards (winning 39): 8 nominations for Iron Man (winning three)")
for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)
