"""
Code Written by Pat Hall
Last Updated: 11.21.23
"""

import json
#opens json file and returns a a parsed python dictionary.
def open_json(fileName):
    with open(fileName, 'r') as fhand:
        parsed_json = json.load(fhand)

    return parsed_json

#Opens .txt file and stores as as a string. 
def read_txt(fileName):
    print("opening " + fileName + "...")
    
    try:
        fhand = open(fileName,'r')
    
    except:
        print("could not locate file " + fileName + " in directory.")
        exit()

    readout = fhand.read()
    sansNewLines = readout.split("\n")

    return sansNewLines

def replace_spaces_in_list(list,char='_'):
    modified_list = [item.replace(" ", char) for item in list]
    return modified_list