---
title: "Localisation"
linkTitle: "Localisation"
weight: 50
---

## Localisation 
```KopiaUI``` supports localisation for different languages since version ```0.17```. The following guideline outlines the localisiation procedure for adding as well as maintaining languages in ```kopiaUI```. 

## Localisation Guidlines

### Localisation Files & Folders
Supported languages are declared in ```languages.json``` along with their meta data. Each language consists of a ```key``` and various meta information that is used within ```kopiaUI```. The file is located under ```src/assets/```.  

- The ```label``` displays the corresponding language as a text in a select component. It is using a native representation of that specific language. 
- The ```code``` is given in upper-case letters. It is responsible for displaying a corresponding national flag next to the selected language. The ```code``` uses the ISO-3166-1 definition.
- The ```value``` attribute is used a an option for the select component. After selecting a specific language, the value is stored in the UI-Preference context and is sent to ```kopia```.

> NOTE: More information on supported ISO-3166-1 codes [can be found here](https://en.wikipedia.org/wiki/ISO_3166-1).

 The following code shows the ```language.json``` file with different languages.  

```json
{
    "en": {
        "label": "English",
        "code": "GB",
        "value": "en"
    },
    "de": {
        "label": "Deutsch",
        "code": "DE",
        "value": "de"
    },
    "es": {
        "label": "Español",
        "code": "ES",
        "value": "es"
    },
    "fr": {
        "label": "Français",
        "code": "FR",
        "value": "fr"
    },
    "ru": {
        "label": "Русский",
        "code": "RU",
        "value": "ru"
    },
    "jp": {
        "label": "日本語",
        "code": "JP",
        "value": "jp"
    },
    "it": {
        "label": "Italiano",
        "code": "IT",
        "value": "it"
    },
    "pl": {
        "label": "Polski",
        "code": "PL",
        "value": "pl"
    }
}
```

The actual translations are located under ```public/locales/```, where each language is located under it's own sub-folder. The translation file itself is a json - consisting of key/value pairs. The ```key``` is a unique identifier that points to a specific item. The ```value``` contains the translated string for that language.  

The following example shows some strings in english language:
```json
{
  "common.back": "Back",
  "common.cancel": "Cancel",
  "common.click-here-to-learn-more": "Click here to learn more",
  "common.delete": "Delete",
  "common.delete-confirm": "Confirm Delete",
  "common.loading": "Loading ...",
  "common.next": "Next",
  "common.return": "Return",
  "common.stop": "Stop"
  [...]
}
```

### Naming Conventions
We defined a strict naming convention to facilitate a meaningful and extendable organization of translations across ```kopiaUI```and ```kopia``` in the future. A proper naming convention is needed to provide context and hints to translators - lowering the chances of misunderstandings and poor translations. The naming convention uses the following schema: ```[type].[category].[sub-category].[id]```

There are 

| Type  | Meaning  |
|---|---|
|```feedback``` | The type ```feedback``` shoule be used when informing or displaying information to the user. For example, these can be hints, placeholders or labels |
|```value```    | The type ```value``` is used to |
|```event```    |   |
|```common```   |   |


| Category  | Meaning  |
|---|---|
|```policy```    | The type ```feedback``` shoule be used when informing or displaying information to the user. For example, these can be hints, placeholders or labels |
|```repository```       | The type ```repository``` is used to    |
|```task```      |   |
|```log```     |   |
|```pin```     |   |
|```validation```     |   |
|```button```     |   |
|```label```     |   |
|```provider```     |   |
|```algorithm```     |   |
|```ui```     |   |
|```ui```     |   |


| Sub-Category  | Meaning  |
|---|---|
|```policy```    | The type ```feedback``` shoule be used when informing or displaying information to the user. For example, these can be hints, 



### Namespaces
TBD

### Tests
```kopiaUI``` features different tests to prevent situations where translations become obsolet and or unused. The tests check for keys with empty or null values and that keys exist in all provided translation files. The tests will fail if translation files contain empty keys or if keys are not declared in all files. Additionally, the test extracts the currently used translation keys within the code to check if all keys are used at least once. 

> Note: It is mandatory that all these tests pass when adding new languages or updating existing keys.

## Supporting Additional Languages
Adding new languages to ```kopiaUI``` is a two-step process. First, the new language has to be declared in the ```language.json``` file. The file has to be updated with a key along with the corresponding attributes of the new language. 

The following example shows the addition of the ```danish``` language. 

```json
{
    "dk": {
        "label": "Dansk",
        "code": "DK",
        "value": "dk"
    }
}
```
Secondly, the translation itself has to be provided. This file should contain a full translation of all ```keys``` that are currently used in ```kopiaUI```. After providing the full translation, ```kopiaUI``` shows the newly added language in the selector automatically. 

> NOTE: As ```english``` is the primary language in ```kopia```, a new translation should incorporate all the keys of that language to provide a full translation.