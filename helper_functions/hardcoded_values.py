# Hard-coded values. Check if up-to-date!
produkte_dict = {
    "29B43D7B-960F-494F-B260-33368AE9ACE2": "116xyz-Kurznummer",
    "A899AEA6-3033-46E5-A242-8A0F3A425EF5": "18xy-Kurznummer",
    "5D4C0871-C63D-49BB-850F-606E70817CE1": "1xy-Kurznummer",
    "D75E0240-0C9D-4DAD-B31D-BF8C5CF86A54": "Carrier Selection Code (CSC)",
    "8D4C2F35-E90A-4D77-8D76-F453A7FF7CBE": "E.164-Nummernblock",
    "FD2FCD2D-37D9-4BA1-A93E-4F8AB22C635A": "E.164-Zugangskennzahl",
    "99FF60D9-9B43-4C47-8948-14EAAE686677": "Einzelnummer",
    "B24663B2-9E1E-40DC-9055-12FB4799CFDF": "International Signalling Point Code (ISPC)",
    "D0679B9A-2EE4-4A23-A32D-814B256B321B": "Issuer Identifier Number (IIN)",
    "315E0746-3047-45F2-B5AC-AFD30F9412E7": "Mobile Network Code (MNC)",
    "EA974444-4AF0-4E8F-86CC-3CF4BB587E26": "National Signalling Point Code (NSPC)",
    "C50E13E2-391C-48D4-96E8-F7EB8D7E30C0": "Objektbezeichner (OID)",
    "D57A30D1-1C04-46CD-9E6C-F66D6A86C55B": "Weiteres Adressierungselement",
    "87CE18A7-A3D5-43E5-A445-72AD21B351FF": "Packet Radio Rufzeichen",
    "31D7DED6-CA00-4309-8529-833272055D5B": "Rufzeichen Amateurfunk",
    "FF1B2DFE-39CE-457C-A0E9-9B5C44FB52CA": "Rufzeichen Hochseeyacht",
    "B015893F-82C4-45E7-AC6D-8F81FC54795E": "Rufzeichen Luftfahrzeug",
    "63FBD550-0EE0-4AD3-9893-DB10855DB242": "Rufzeichen Rheinschiff",
    "B90ED2E5-14EA-4539-B4E6-FABFC915A113": "Rufzeichen SOLAS-Schiff",
    "9EDF7CB3-33E9-4743-98C9-E0B6B87F2EDF": "Handsprechfunkgeräte mit DSC (Maritime Kennung)",
    "978F554D-5DD4-4FA7-8654-E099D56304C2": "FDA",
}
produkte_dict_name_first = {
    "116xyz-Kurznummer": "29B43D7B-960F-494F-B260-33368AE9ACE2",
    "18xy-Kurznummer": "A899AEA6-3033-46E5-A242-8A0F3A425EF5",
    "1xy-Kurznummer": "5D4C0871-C63D-49BB-850F-606E70817CE1",
    "Carrier Selection Code (CSC)": "D75E0240-0C9D-4DAD-B31D-BF8C5CF86A54",
    "E.164-Nummernblock": "8D4C2F35-E90A-4D77-8D76-F453A7FF7CBE",
    "E.164-Zugangskennzahl": "FD2FCD2D-37D9-4BA1-A93E-4F8AB22C635A",
    "Einzelnummer": "99FF60D9-9B43-4C47-8948-14EAAE686677",
    "International Signalling Point Code (ISPC)": "B24663B2-9E1E-40DC-9055-12FB4799CFDF",
    "Issuer Identifier Number (IIN)": "D0679B9A-2EE4-4A23-A32D-814B256B321B",
    "Mobile Network Code (MNC)": "315E0746-3047-45F2-B5AC-AFD30F9412E7",
    "National Signalling Point Code (NSPC)": "EA974444-4AF0-4E8F-86CC-3CF4BB587E26",
    "Objektbezeichner (OID)": "C50E13E2-391C-48D4-96E8-F7EB8D7E30C0",
    "Weiteres Adressierungselement": "D57A30D1-1C04-46CD-9E6C-F66D6A86C55B",
    "Packet Radio Rufzeichen": "87CE18A7-A3D5-43E5-A445-72AD21B351FF",
    "Rufzeichen Amateurfunk": "31D7DED6-CA00-4309-8529-833272055D5B",
    "Rufzeichen Hochseeyacht": "FF1B2DFE-39CE-457C-A0E9-9B5C44FB52CA",
    "Rufzeichen Luftfahrzeug": "B015893F-82C4-45E7-AC6D-8F81FC54795E",
    "Rufzeichen Rheinschiff": "63FBD550-0EE0-4AD3-9893-DB10855DB242",
    "Rufzeichen SOLAS-Schiff": "B90ED2E5-14EA-4539-B4E6-FABFC915A113",
    "Handsprechfunkgeräte mit DSC (Maritime Kennung)": "9EDF7CB3-33E9-4743-98C9-E0B6B87F2EDF",
    "FDA": "978F554D-5DD4-4FA7-8654-E099D56304C2",
}


# Ausweis applies only to Personen.
servicerollen = {
    "AC441A4D-0BB7-4363-ACFF-DFAEECF2AF12": "FDA",
    "0DA55C52-B526-4FEC-A663-5AC9919B1C9D": "Veranstalterkonzessionär",
    "C440845B-1DA5-4663-B37C-BD3E0466E9A8": "BORS",
    "393C48AA-2986-4F08-BADE-D97ADE0BB332": "Ausweis",
}


# Personen können Kontaktperson, Statistikperson, Technikperson für folgende Produkte sein:
produkte_dict_personen = {
    "B015893F-82C4-45E7-AC6D-8F81FC54795E": "Rufzeichen Luftfahrzeug",
    "63FBD550-0EE0-4AD3-9893-DB10855DB242": "Rufzeichen Rheinschiff",
    "B90ED2E5-14EA-4539-B4E6-FABFC915A113": "Rufzeichen SOLAS-Schiff",
    "978F554D-5DD4-4FA7-8654-E099D56304C2": "FDA",
}
