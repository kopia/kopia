---
title: "Localisation"
linkTitle: "Localisation"
weight: 50
---

## Localisation 
```KopiaUI``` supports localisation for different languages since version ```0.17```. The following guideline outlines the localisiation procedure for adding as well as maintaining languages in ```kopiaUI```. 

## Localisation Guidlines

### Localisation Files & Folders
The file ```languages.json``` is a json representation of supported languages along with their meta data. The ```label``` is used to display the corresponding language as text in a react-select component. It is using a native representation of the language. 

The ```code``` contains ISO-3166-1 codes and is given in upper-case letters. It is responsible for displaying a corresponding national flag next to the selected language. The ```code``` uses a ISO-3166-1 representation of the language. 

> NOTE: More information on supported ISO-3166-1 codes [can be found here](https://en.wikipedia.org/wiki/ISO_3166-1).

Finally, the ```value``` attribute is used a an option for the react-select component. After selecting a specific language, the value is stored in the UI-Preference context and is sent to ```kopia```. The following code shows the ```language.json``` file with different languages.  

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

### Naming Conventions
TBD

### Namespaces
```json
{
  "common.back": "Back",
  "common.cancel": "Cancel",
  "common.click-here-to-learn-more": "Click here to learn more.",
  "common.delete": "Delete",
  "common.delete-confirm": "Confirm Delete",
  "common.loading": "Loading ...",
  "common.next": "Next",
  "common.return": "Return",
  "common.stop": "Stop"
  [...]
}
```

### Tests
```kopiaUI``` features different tests to prevent situations where translations become obsolet and or unused. The tests check for keys with empty or null values and that keys exist in all provided translation files. The tests will fail if translation files contain empty keys or if keys are not declared in all files. 

Additionally, the test extracts the currently used translation keys within the code to check if all keys are used at least once. 

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