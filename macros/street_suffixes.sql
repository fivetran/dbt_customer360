{%- macro street_suffixes(address_line_1) -%}

{%- set suffix_dict = {
    'Wells': 'Wells',
    'Wells': 'Wls',
    'Well': 'Well',
    'Ways': 'Ways',
    'Way': 'Wy',
    'Way': 'Way',
    'Wall': 'Wall',
    'Walks': 'Walks',
    'Walk': 'Walk',
    'Vista': 'Vista',
    'Vista': 'Vist',
    'Vista': 'Vis',
    'Vista': 'Vsta',
    'Vista': 'Vst',
    'Ville': 'Ville',
    'Ville': 'Vl',
    'Villages': 'Villages',
    'Villages': 'Vlgs',
    'Village': 'Villg',
    'Village': 'Villiage',
    'Village': 'Village',
    'Village': 'Villag',
    'Village': 'Vill',
    'Village': 'Vlg',
    'Views': 'Views',
    'Views': 'Vws',
    'View': 'View',
    'View': 'Vw',
    'Viaduct': 'Vdct',
    'Viaduct': 'Viadct',
    'Viaduct': 'Viaduct',
    'Viaduct': 'Via',
    'Vereda': 'Ver',
    'Valleys': 'Valleys',
    'Valleys': 'Vlys',
    'Valley': 'Valley',
    'Valley': 'Vally',
    'Valley': 'Vlly',
    'Valley': 'Vly',
    'Unions': 'unions',
    'Union': 'union',
    'Union': 'un',
    'Underpass': 'underpass',
    'Turnpike': 'Trnpk',
    'Turnpike': 'Turnpike',
    'Turnpike': 'Turnpk',
    'Tunnel': 'Tunls',
    'Tunnel': 'Tunnels',
    'Tunnel': 'Tunnel',
    'Tunnel': 'Tunnl',
    'Tunnel': 'Tunel',
    'Tunnel': 'Tunl',
    'Trailer': 'Trailer',
    'Trailer': 'Trlrs',
    'Trailer': 'Trlr',
    'Trail': 'Trails',
    'Trail': 'Trail',
    'Trail': 'Trls',
    'Trail': 'Trl',
    'Trafficway': 'Trafficway',
    'Track': 'Tracks',
    'Track': 'Track',
    'Track': 'Trak',
    'Track': 'Trks',
    'Track': 'Trk',
    'Trace': 'Traces',
    'Trace': 'Trace',
    'Trace': 'Trce',
    'Throughway': 'Throughway',
    'Terrace': 'Terrace',
    'Terrace': 'Terr',
    'Terrace': 'Ter',
    'Summit': 'Smt',
    'Summit': 'Sumitt',
    'Summit': 'Summit',
    'Summit': 'Sumit',
    'Streets': 'Streets',
    'Street': 'Street',
    'Street': 'Strt',
    'Street': 'Str',
    'Street': 'St',
    'Stream': 'Stream',
    'Stream': 'Streme',
    'Stream': 'Strm',
    'Stravenue': 'Stravn',
    'Stravenue': 'Stravenue',
    'Stravenue': 'Straven',
    'Stravenue': 'Strav',
    'Stravenue': 'Stra',
    'Stravenue': 'Strvnue',
    'Stravenue': 'Strvn',
    'Station': 'Station',
    'Station': 'Statn',
    'Station': 'Stn',
    'Station': 'Sta',
    'Squares': 'Sqrs',
    'Squares': 'Squares',
    'Square': 'Sqre',
    'Square': 'Sqr',
    'Square': 'Square',
    'Square': 'Squ',
    'Square': 'Sq',
    'Spurs': 'Spurs',
    'Spur': 'Spur',
    'Springs': 'Spgs',
    'Springs': 'Spngs',
    'Springs': 'Springs',
    'Springs': 'Sprngs',
    'Spring': 'Spg',
    'Spring': 'Spng',
    'Spring': 'Spring',
    'Spring': 'Sprng',
    'Skyway': 'Skyway',
    'Shores': 'Shoars',
    'Shores': 'Shores',
    'Shores': 'Shrs',
    'Shore': 'Shoar',
    'Shore': 'Shore',
    'Shore': 'Shr',
    'Shoals': 'Shls',
    'Shoals': 'Shoals',
    'Shoal': 'Shl',
    'Shoal': 'Shoal',
    'Run': 'Run',
    'Rue': 'Rue',
    'Row': 'Row',
    'Route': 'Route',
    'Roads': 'Roads',
    'Roads': 'Rds',
    'Road': 'Rd',
    'Road': 'Road',
    'River': 'River',
    'River': 'Riv',
    'River': 'Rvr',
    'River': 'Rivr',
    'Ridges': 'Rdgs',
    'Ridges': 'Ridges',
    'Ridge': 'Rdge',
    'Ridge': 'Rdg',
    'Ridge': 'Ridge',
    'Rest': 'Rest',
    'Rest': 'Rst',
    'Rapids': 'Rapids',
    'Rapids': 'Rpds',
    'Rapid': 'Rapid',
    'Rapid': 'Rpd',
    'Rancho': 'Rch',
    'Ranch': 'Ranches',
    'Ranch': 'Ranch',
    'Ranch': 'Rnchs',
    'Ranch': 'Rnch',
    'Ramp': 'Ramp',
    'Radial': 'Radial',
    'Radial': 'Radiel',
    'Radial': 'Radl',
    'Radial': 'Rad',
    'Prairie': 'Prairie',
    'Prairie': 'Prr',
    'Ports': 'Ports',
    'Ports': 'Prts',
    'Port': 'Port',
    'Port': 'Prt',
    'Prairie': 'Pr',
    'Points': 'Points',
    'Points': 'Pts',
    'Point': 'Point',
    'Point': 'Pt',
    'Plaza': 'Plaza',
    'Plaza': 'Plza',
    'Plaza': 'Plz',
    'Plains': 'Plains',
    'Plains': 'Plns',
    'Plain': 'Plain',
    'Plain': 'Pln',
    'Placita': 'Pla',
    'Place': 'Pl',
    'Pines': 'Pines',
    'Pines': 'Pnes',
    'Pine': 'Pine',
    'Pike': 'Pikes',
    'Pike': 'Pike',
    'Path': 'Paths',
    'Path': 'Path',
    'Passage': 'Passage',
    'Pass': 'Pass',
    'Paseo': 'Pso',
    'Parkways': 'Parkways',
    'Parkways': 'Pkwys',
    'Parkway': 'Parkway',
    'Parkway': 'Parkwy',
    'Parkway': 'Pkway',
    'Parkway': 'Pkwy',
    'Parkway': 'Pky',
    'Parks': 'Parks',
    'Park': 'Park',
    'Park': 'Prk',
    'Overpass': 'Overpass',
    'Oval': 'Oval',
    'Oval': 'Ovl',
    'Orchard': 'Orchard',
    'Orchard': 'Orchrd',
    'Orchard': 'Orch',
    'Neck': 'Nck',
    'Neck': 'Neck',
    'Mountains': 'Mntns',
    'Mountains': 'Mountains',
    'Mountain': 'Mntain',
    'Mountain': 'Mntn',
    'Mountain': 'Mountain',
    'Mountain': 'Mountin',
    'Mountain': 'Mtin',
    'Mountain': 'Mtn',
    'Mount': 'Mnt',
    'Mount': 'Mt',
    'Mount': 'Mount',
    'Motorway': 'Motorway',
    'Mission': 'Missn',
    'Mission': 'Mssn',
    'Mills': 'Mills',
    'Mill': 'Mill',
    'Mews': 'Mews',
    'Meadows': 'Mdws',
    'Meadows': 'Mdw',
    'Meadows': 'Meadows',
    'Meadows': 'Medows',
    'Meadow': 'Meadow',
    'Manors': 'Manors',
    'Manors': 'Mnrs',
    'Manor': 'Mnr',
    'Manor': 'Manor',
    'Mall': 'Mall',
    'Loop': 'Loops',
    'Loop': 'Loop',
    'Lodge': 'Ldge',
    'Lodge': 'Ldg',
    'Lodge': 'Lodge',
    'Lodge': 'Lodg',
    'Locks': 'Lcks',
    'Locks': 'Locks',
    'Lock': 'Lck',
    'Lock': 'Lock',
    'Loaf': 'Lf',
    'Loaf': 'Loaf',
    'Lights': 'Lights',
    'Light': 'Lgt',
    'Light': 'Light',
    'Lane': 'Lane',
    'Lane': 'Ln',
    'Landing': 'Landing',
    'Landing': 'Lndg',
    'Landing': 'Lndng',
    'Land': 'Land',
    'Lakes': 'Lks',
    'Lakes': 'Lakes',
    'Lake': 'Lk',
    'Lake': 'Lake',
    'Knolls': 'Knls',
    'Knolls': 'Knolls',
    'Knoll': 'Knl',
    'Knoll': 'Knoll',
    'Knoll': 'Knol',
    'Keys': 'Keys',
    'Keys': 'Kys',
    'Key': 'Key',
    'Key': 'Ky',
    'Junctions': 'Jctns',
    'Junctions': 'Jcts',
    'Junctions': 'Junctions',
    'Junction': 'Jction',
    'Junction': 'Jctn',
    'Junction': 'Jct',
    'Junction': 'Junction',
    'Junction': 'Junctn',
    'Junction': 'Juncton',
    'Isle': 'Isles',
    'Isle': 'Isle',
    'Islands': 'Islands',
    'Islands': 'Islnds',
    'Islands': 'Iss',
    'Island': 'Island',
    'Island': 'Islnd',
    'Island': 'Is',
    'Inlet': 'Inlt',
    'Hollow': 'Hllw',
    'Hollow': 'Hollows',
    'Hollow': 'Hollow',
    'Hollow': 'Holws',
    'Hollow': 'Holw',
    'Hills': 'Hills',
    'Hills': 'Hls',
    'Hill': 'Hill',
    'Hill': 'Hl',
    'Highway': 'Highway',
    'Highway': 'Highwy',
    'Highway': 'Hiway',
    'Highway': 'Hiwy',
    'Highway': 'Hway',
    'Highway': 'Hwy',
    'Heights': 'Hts',
    'Heights': 'Ht',
    'Haven': 'Haven',
    'Haven': 'Hvn',
    'Harbors': 'Harbors',
    'Harbor': 'Harbor',
    'Harbor': 'Harbr',
    'Harbor': 'Harb',
    'Harbor': 'Hbr',
    'Harbor': 'Hrbor',
    'Groves': 'Groves',
    'Grove': 'Grove',
    'Grove': 'Grov',
    'Grove': 'Grv',
    'Greens': 'Greens',
    'Green': 'Green',
    'Green': 'Grn',
    'Glens': 'Glens',
    'Glen': 'Glen',
    'Glen': 'Gln',
    'Gateway': 'Gateway',
    'Gateway': 'Gatewy',
    'Gateway': 'Gatway',
    'Gateway': 'Gtway',
    'Gateway': 'Gtwy',
    'Gardens': 'Gardens',
    'Gardens': 'Gdns',
    'Gardens': 'Grdns',
    'Garden': 'Garden',
    'Garden': 'Gardn',
    'Garden': 'Grden',
    'Garden': 'Grdn',
    'Freeway': 'Freeway',
    'Freeway': 'Freewy',
    'Freeway': 'Frway',
    'Freeway': 'Frwy',
    'Freeway': 'Fwy',
    'Fort': 'Fort',
    'Fort': 'Frt',
    'Fort': 'Ft',
    'Forks': 'Forks',
    'Forks': 'Frks',
    'Fork': 'Fork',
    'Fork': 'Frk',
    'Forges': 'Forges',
    'Forge': 'Forge',
    'Forge': 'Frg',
    'Forge': 'Forg',
    'Forest': 'Forests',
    'Forest': 'Forest',
    'Forest': 'Frst',
    'Fords': 'Fords',
    'Ford': 'Ford',
    'Ford': 'Frd',
    'Flats': 'Flats',
    'Flats': 'Flts',
    'Flat': 'Flat',
    'Flat': 'Flt',
    'Fields': 'Fields',
    'Fields': 'Flds',
    'Field': 'Field',
    'Field': 'Fld',
    'Ferry': 'Ferry',
    'Ferry': 'Frry',
    'Ferry': 'Fry',
    'Falls': 'Falls',
    'Falls': 'Fls',
    'Fall': 'Fall',
    'Extensions': 'Exts',
    'Extension': 'Ext',
    'Extension': 'Extension',
    'Extension': 'Extn',
    'Extension': 'Extnsn',
    'Expressway': 'Expr',
    'Expressway': 'Expressway',
    'Expressway': 'Express',
    'Expressway': 'Expw',
    'Expressway': 'Expy',
    'Expressway': 'Exp',
    'Estates': 'Estates',
    'Estates': 'Ests',
    'Estate': 'Estate',
    'Estate': 'Est',
    'Entrada': 'Ent',
    'Drives': 'Drives',
    'Drive': 'Drive',
    'Drive': 'Driv',
    'Drive': 'Drv',
    'Drive': 'Dr',
    'Divide': 'Divide',
    'Divide': 'Div',
    'Divide': 'Dvd',
    'Divide': 'Dv',
    'Dam': 'Dam',
    'Dam': 'Dm',
    'Dale': 'Dale',
    'Dale': 'Dl',
    'Curve': 'Curve',
    'Crossroads': 'Crossroads',
    'Crossroad': 'Crossroad',
    'Crossing': 'Crossing',
    'Crossing': 'Crssng',
    'Crossing': 'xing',
    'Crest': 'Crest',
    'Crescent': 'Crescent',
    'Crescent': 'Cres',
    'Crescent': 'Crsent',
    'Crescent': 'Crsnt',
    'Creek': 'Creek',
    'Creek': 'Crk',
    'Coves': 'Coves',
    'Cove': 'Cove',
    'Cove': 'Cv',
    'Courts': 'Courts',
    'Courts': 'Cts',
    'Court': 'Court',
    'Court': 'Ct',
    'Course': 'Course',
    'Course': 'Crse',
    'Corners': 'Corners',
    'Corners': 'Cors',
    'Corner': 'Corner',
    'Corner': 'Cor',
    'Commons': 'Commons',
    'Common': 'Common',
    'Club': 'Clb',
    'Club': 'Club',
    'Cliffs': 'Clfs',
    'Cliffs': 'Cliffs',
    'Cliff': 'Clf',
    'Cliff': 'Cliff',
    'Circles': 'Circles',
    'Circle': 'Circle',
    'Circle': 'Circl',
    'Circle': 'Circ',
    'Circle': 'Cir',
    'Circle': 'Crcle',
    'Circle': 'Crcl',
    'Cerrada': 'Cer',
    'Centers': 'Centers',
    'Center': 'Center',
    'Center': 'Centre',
    'Center': 'Centr',
    'Center': 'Cent',
    'Center': 'Cen',
    'Center': 'Cnter',
    'Center': 'Cntr',
    'Center': 'Ctr',
    'Causeway': 'Causeway',
    'Causeway': 'Causwa',
    'Causeway': 'Cswy',
    'Cape': 'Cape',
    'Cape': 'Cpe',
    'Canyon': 'Canyn',
    'Canyon': 'Canyon',
    'Canyon': 'Cnyn',
    'Camp': 'Camp',
    'Camp': 'Cp',
    'Camp': 'Cmp',
    'Camino': 'Cam',
    'Caminito': 'Cmt',
    'Calle': 'Cll',
    'Bypass': 'Bypass',
    'Bypass': 'Bypas',
    'Bypass': 'Bypa',
    'Bypass': 'Byps',
    'Bypass': 'Byp',
    'Burgs': 'Burgs',
    'Burg': 'Burg',
    'Brooks': 'Brooks',
    'Brook': 'Brk',
    'Brook': 'Brook',
    'Bridge': 'Brdge',
    'Bridge': 'Brg',
    'Bridge': 'Bridge',
    'Branch': 'Brnch',
    'Branch': 'Branch',
    'Branch': 'Br',
    'Boulevard': 'Blvd',
    'Boulevard': 'Boulevard',
    'Boulevard': 'Boulv',
    'Boulevard': 'Boul',
    'Bottom': 'Bottm',
    'Bottom': 'Bottom',
    'Bottom': 'Bot',
    'Bottom': 'Btm',
    'Bluffs': 'Bluffs',
    'Bluff': 'Blf',
    'Bluff': 'Bluff',
    'Bluff': 'Bluf',
    'Bend': 'Bend',
    'Bend': 'Bnd',
    'Beach': 'Bch',
    'Beach': 'Beach',
    'Bayou': 'Bayoo',
    'Bayou': 'Bayou',
    'Avenue': 'Avenue',
    'Avenue': 'Avenu',
    'Avenue': 'Aven',
    'Avenue': 'Ave',
    'Avenue': 'Avn',
    'Avenue': 'Avnue',
    'Avenue': 'Av',
    'Arcade': 'Arcade',
    'Arcade': 'Arc',
    'Anex': 'Anex',
    'Anex': 'Annex',
    'Anex': 'Annx',
    'Anex': 'Anx',
    'Alley': 'Allee',
    'Alley': 'Alley',
    'Alley': 'Ally',
    'Alley': 'Aly'
}
-%}

case
{% for longname, abbreviation in suffix_dict.items() %}
    when lower( {{ address_line_1 }} ) like '% % {{ abbreviation|lower }}' 
        or lower( {{ address_line_1 }} ) like '% % {{ abbreviation|lower }} %'
        or lower({{ address_line_1 }}) like '% % {{ abbreviation|lower }},%' 
    then replace(replace(replace({{ address_line_1 }}, '{{ abbreviation }}', '{{ longname }}'), '{{ abbreviation|lower }}', '{{ longname }}'), '{{ abbreviation|upper }}', '{{ longname }}')
{%- endfor -%}
    else {{ address_line_1 }} 
end as {{ address_line_1 }}_long

{%- endmacro -%}