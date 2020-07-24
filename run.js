// jshint esversion: 6
const fs = require('fs');
const path = require('path');

const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify/lib/sync');
const argv = require('minimist')(process.argv.slice(2));
const zeroFill = require('zero-fill');
const moment = require('moment');

const combined_output_headers = (argv.combined_output_headers || 'Include	Reaccession Number	Name	Sample_number	Strain ID	Genus	species	subspecies	serover	OWNER	Collected by	collection_year	collection_month	collection_day	collection_source	Country	State	Other_Location_Information		NCBI_Sample_Type	Specific_Host	Host_Disease	PN_source_Type		Culture collection Inst.	culture collection ID	NCBI Type strain	WGS PulseNet ID	FACTS_ID	NARMS ID	CDC ID	Isolate_Name_Alias	PrivateStrainSynomyms		Pathotype	Phagetype	Toxin	PFGE_PrimaryEnzyme_pattern	PFGE_SecondaryEnzyme_pattern	VirulenceMarker		NCBI BioProject ID	Project	PulseNet_Outbreak_Code	Isolate_contributor	Public Comments	Private Comments	Metadata Issues		VetLIRN_SourceLab	Method used for organism identification	Isolation_Plate	Isolation_Plate_Other	Case_type	VetLIRN_Salmonella_serotype	VetLIRN_CollectionSource	VetLIRN_CollectionSourceComment').split("\t");

const lastRelevantHeaderIndex = combined_output_headers.indexOf('VetLIRN_CollectionSourceComment');

const plateIndex = combined_output_headers.indexOf('Isolation_Plate');
const strainIdIndex = combined_output_headers.indexOf('Strain ID');
const genusIndex = combined_output_headers.indexOf('Genus');
const speciesIndex = combined_output_headers.indexOf('species');
const seroverIndex = combined_output_headers.indexOf('serover');

const include_header_name = argv.include_header || 'Include';
const input_data_folder = argv.folder || 'C:\\Users\\msp13\\Desktop\\VETLIRNMasterList';
const combined_isolates_filename = argv.combined || `Vet-LIRN_metadata_GT-v3.4-V-v14.csv`;
const sensititre_filename = argv.sensititre || `SWINExportFile.TXT`;
const output_filename = argv.output_filename || argv.o || 'output.csv';

// for name generation
let combined_isolates_csv = fs.readFileSync(path.join(input_data_folder, combined_isolates_filename), 'utf8');

// pre-process the data to remove everything up to the first 'YES,'
combined_isolates_csv = combined_isolates_csv.slice(combined_isolates_csv.toLowerCase().indexOf('yes,'));
// pad the header with spaces as necessary
const data_record_length = combined_isolates_csv.split('\n').map(v => v.split(','))[1].length;
while(combined_output_headers.length < data_record_length) {
    combined_output_headers.push('');
}
combined_isolates_csv = combined_output_headers.join(',') + '\n' + combined_isolates_csv;

const sensititre_csv = fs.readFileSync(path.join(input_data_folder, sensititre_filename), 'utf16le').replace(/[\t]+/g, '\t').replace(/[\u0000]+/g, ''); // remove consecutive delimieters
//Added BOPO7F and EQUIN2F plates 5/27/20
const atb_plate_drug_map = {
    'BOPO6F':  ['AMPICI','CEFTIF','CHLTET','CLINDA','DANOFL','ENROFL','FLORFE','GENTAM','NEOMYC','OXYTET','PENICI','SDIMET','SPECT','TIAMUL','TILMIC','TRISUL','TULATH','TYLO'],
    'BOPO7F': ['AMPICI','CEFTIF','CLINDA','DANOFL','ENROFL','FLORFE','GAMITH','GENTAM','NEOMYC','PENICI','SDIMET','SPECT','TETRA','TIAMUL','TILDIP','TILMIC','TRISUL','TULATH','TYLO'],
    'AVIAN1F':  ['AMOXIC','CEFTIF','CLINDA','ENROFL','ERYTH','FLORFE','GENTAM','NEOMYC','NOVOBI','OXYTET','PENICI','SDIMET','SPECT','STREPT','SULTHI','TETRA','TRISUL','TYLO'],
    'EQUIN1F': ['AMIKAC','AMPICI','AZITHR','CEFAZO','CEFTAZ','CEFTIF','CHLORA','CLARYT','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','OXACIL','PENICI','RIFAMP','TETRA','TICARC','TICCLA','TRISUL'],
    'EQUIN2F': ['AMIKAC','AMPICI','CEFAZO','CEFTAZ','CEFTIF','CHLORA','CLARYT','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','MINOCY','OXACIL','PENICI','RIFAMP','TETRA','TRISUL'],
    'COMPGN1F': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEFTAZ','CEPALE','CHLORA','DOXYCY','ENROFL','GENTAM','IMIPEN','MARBOF','ORBIFL','PIPTAZ','PRADOF','TETRA','TRISUL'],
    'COMPGP1F': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEPHAL','CHLORA','CLINDA','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','MARBOF','MINOCY','NITRO','OXACIL','PENICI','PRADOF','RIFAMP','TETRA','TRISUL','VANCOM'],
    'CMV1BURF': ['AMOCLA','AMPICI','CEFTIF','CEPALE','ENROFL','TETRA','TRISUL'],
    'OTHER': ['AMIKAC','BACITR','CEFAZO','CEFTIF','CHLORA','CIPROF','DOXYCY','ERYTH','GENTAM','MOXIFL','NEOMYC','OFLOXA','OXYTET','POLYB','TICARC','TOBRAM','TRISUL']
};

const missingATBs = new Map();
const missingMICs = new Map();

// pre-process combined isolates data
let combined_isolates_data = parse(combined_isolates_csv, {columns: true});
combined_isolates_data = combined_isolates_data.filter(r => r[include_header_name].toLowerCase() === 'yes');
console.log(`${combined_isolates_data.length} accessions will be included:`);

if(combined_isolates_data.length === 0) {
  process.exit(1);
}

const num_input_file_fields = Object.keys(combined_isolates_data[0]).length;

// pre-process sensititre data
let sensititre_data;
try {
    sensititre_data = parse(sensititre_csv, {delimiter: '\t'});
}
catch(e){
    console.log('Sensitre Data parse error', e.message);
    process.exit(1);
}
const sensititre_violations = sensititre_data.filter(d => d.length !== 340);
if(sensititre_violations.length > 0){
    console.dir(sensititre_violations);
    console.error(`Sensitre data has ${sensititre_violations.length} rows with non-compliant column length`);
    process.exit(1);
}

let post_sensitire_data = sensititre_data.map(r => {
    let drug_data = r.slice(40);
    let consolidated_drug_data = [];
    // there are 100 wells worth of data, 3 columns per well
    for(let i = 0; i < 100; i++){
        const base = i * 3;
        const a = drug_data[base], b = drug_data[base+1], c = drug_data[base+2];
        if(a.trim() || b.trim() || c.trim()){
            consolidated_drug_data = consolidated_drug_data.concat([a,b,c]);
        }

    }
    return [].concat(consolidated_drug_data).concat([r[9]]);
});

let errorFlag = false;
let allOutputDataRows = combined_isolates_data.map((r, idx) => {
    let row = combined_output_headers.map(h => r[h] || '');

    const accession_number = row[combined_output_headers.indexOf('Reaccession Number')].trim() ||
      row[combined_output_headers.indexOf('Name')].trim();

    let corresponding_sensitire_row = sensititre_data.findIndex(s => s[6] === accession_number); // 6 is 'column G' in the sensititre data
    if(corresponding_sensitire_row < 0){
        console.error(`Can't find sensititre record for Accesssion #: '${accession_number}'`);
        if(!accession_number) {
            console.error('ROW: ' + row.join(','));
        }
        errorFlag = true;
    } else {
        return row.concat(post_sensitire_data[corresponding_sensitire_row]);
    }
});

if(errorFlag) {
    process.exit(2);
}

allOutputDataRows = allOutputDataRows.filter(r => r);

let allOutputDataRowsByPlateType = {};
allOutputDataRows.forEach(r => {
    let plateType = r[plateIndex];
    if(!allOutputDataRowsByPlateType[plateType]){
      allOutputDataRowsByPlateType[plateType] = [];
    }
    allOutputDataRowsByPlateType[plateType].push(r);
});

console.dir(Object.keys(allOutputDataRowsByPlateType)
    .map((k) => {
        return {
            plateType: k,
            count: allOutputDataRowsByPlateType[k].length
        };
    })
);

const atb_offset = lastRelevantHeaderIndex + 3; // 57; // num_input_file_fields;

Object.keys(allOutputDataRowsByPlateType).forEach((plateType) => {
    // console.log(`Before expandPlateTypeRows, plate type ${plateType} had ${allOutputDataRowsByPlateType[plateType].length} rows`)
    const plate_drug_map = atb_plate_drug_map[plateType];
    const rows = allOutputDataRowsByPlateType[plateType];
    delete allOutputDataRowsByPlateType[plateType];
    expandPlateTypeRows(plateType, rows, plate_drug_map);
});

function expandPlateTypeRows(plateType, rows, plate_drug_map){
    const num_target_drugs = plate_drug_map.length;
    if(num_target_drugs === 0){
        console.log(`Warning: Plate Type '${plateType}' has no drug map`);
        return;
    }
    const newRows = rows.map((row, idx) => {
        const targetDrugContent = Array(num_target_drugs * 3).fill('');
        for(let i = atb_offset; row[i] && row[i+1] && row[i+2] && row[i].trim() && row[i+1].trim() && row[i+2].trim(); i += 3){
            let a = row[i], b = row[i+1], c = row[i+2];

            if(!a || !a.trim()) continue;
            a = a.trim(); b = b.trim(); c = c.trim();

            const drugIndex = plate_drug_map.indexOf(a);
            if(drugIndex < 0){
                // const indexOfAccession = Object.keys(accession_number_specimen_id_map).map(k => accession_number_specimen_id_map[k]).indexOf(row[1]);
                // const accessionNumber = Object.keys(accession_number_specimen_id_map)[indexOfAccession];
                const accessionNumber = row[1] || row[2];
                console.error(`Encountered unknown drug '${a}' in Plate Type '${plateType}' Sensititre data for Accession # ${accessionNumber}`, i-atb_offset);
                // console.log(row.slice(atb_offset));
                process.exit(3);
            }
            const base = drugIndex * 3;
            targetDrugContent[base] = a;
            targetDrugContent[base+1] = b;
            targetDrugContent[base+2] = c;
        }

        for(let i = 0; i < plate_drug_map.length; i++){
            const atb = (targetDrugContent[i*3] || "").trim();
            const mic = (targetDrugContent[i*3 + 1] || "").trim();
            if(plate_drug_map[i]){
                // const indexOfAccession = Object.keys(accession_number_specimen_id_map).map(k => accession_number_specimen_id_map[k]).indexOf(row[1]);
                // const accessionNumber = Object.keys(accession_number_specimen_id_map)[indexOfAccession];
                const accessionNumber = row[1] || row[2];
                if(!atb){
                    console.error(`WARNING: Accession # ${accessionNumber} is missing ATB '${plate_drug_map[i]}' -- Genus '${row[5]}' and Plate '${row[51]}'`);
                    missingATBs.set(plate_drug_map[i], missingATBs.get(plate_drug_map[i]) ? missingATBs.get(plate_drug_map[i])+1 : 1)
                } else if(!mic) {
                    console.error(`WARNING: Accession # ${accessionNumber} is missing MIC for ATB '${plate_drug_map[i]}' -- Genus '${row[5]}' and Plate '${row[51]}'`);
                    missingMICs.set(plate_drug_map[i], missingMICs.get(plate_drug_map[i]) ? missingMICs.get(plate_drug_map[i])+1 : 1)
                }
            }
        }

        // const newRow = row.slice(0, atb_offset).concat(targetDrugContent).map(v => `"=""${v}"""`);
        const newRow = row.slice(0, atb_offset).concat(targetDrugContent).concat(row.slice(-1)[0]);
        return newRow;
    }).filter(v => v);

    effectivePlateTypeTarget = allOutputDataRowsByPlateType;
    if(!Array.isArray(plateType)){
        plateType = [plateType];
    }
    while(plateType.length != 1){
        const s = plateType.shift();
        effectivePlateTypeTarget = effectivePlateTypeTarget[s];
    }
    plateType = plateType.shift();
    if(!allOutputDataRowsByPlateType[plateType]){
    //   console.log(`Creating new Plate Type entry: ${plateType}`)
      allOutputDataRowsByPlateType[plateType] = [];
    }
    // console.log(`Adding ${newRows.length} rows to Plate type ${plateType}`)
    allOutputDataRowsByPlateType[plateType] = allOutputDataRowsByPlateType[plateType].concat(newRows);
}

console.log('These files will be generated: ');
console.log(Object.keys(allOutputDataRowsByPlateType).map(k => `${k}.txt`));
Object.keys(allOutputDataRowsByPlateType).forEach((k) => {
    // GENERATE ONE FILE PER PLATE TYPE BY ONLY KEEPING
    // sample ID*	=> Strain ID
    // organism* => Genus + ' ' + species + [' ' serover]

    const plateTypeOutputFileRows = allOutputDataRowsByPlateType[k]
      .map(r => {
        let nr = [];
        nr.push(r[strainIdIndex]);
        // nr.push(r[genusIndex].trim() + ' ' + r[speciesIndex].trim() + ' ' + (r[seroverIndex].trim() ? r[seroverIndex].trim() : ''));
        nr.push(r.pop());
        nr = nr.concat(r.slice(num_input_file_fields + 4)).map(v => v === '\r' ? '' : v);

        return nr;
      });

    fs.writeFileSync(path.join(input_data_folder, `${k}.txt`),
      stringify(plateTypeOutputFileRows, {delimiter: '\t', escape: false, quote: false, quotedString: false, quotedEmpty: false })
    );
});

let cumulative_counts_by_plateType = {};
let total_samples = 0;
combined_isolates_data = combined_isolates_data.map(r => {
    if(r[include_header_name]){
        total_samples++;
        const plateType = r.Isolation_Plate;

        if(!cumulative_counts_by_plateType[plateType]){ cumulative_counts_by_plateType[plateType] = 0; }
        cumulative_counts_by_plateType[plateType]++;
    }

    return r;
});

let target_fields = 'Name,Sample_number,Strain ID,Genus,species,subspecies,serover,,Collected by,collection_year,collection_month,collection_source,Country,State,,NCBI_Sample_Type,Specific_Host,Host_Disease,,VetLIRN_SourceLab,Method used for organism identification,Isolation_Plate,Isolation_Plate_Other,Case_type,VetLIRN_Salmonella_serotype,VetLIRN_CollectionSource,VetLIRN_CollectionSourceComment';
target_fields = target_fields.split(',');
const outputFileRows = allOutputDataRows.map(r => {
  let nr = [];
  target_fields.forEach(field => {
    if(field === '') {
      nr.push('');
    } else {
      const fieldIndex = combined_output_headers.indexOf(field)
      nr.push(r[fieldIndex]);
    }
  });
  return nr;
});

fs.writeFileSync(path.join(input_data_folder, output_filename), stringify(outputFileRows, {header: false}));

// console.log(`These ATBs were missing on at least one accession`, JSON.stringify(Array.from(missingATBs).map(v => `${v[0]}: ${v[1]}`), null, 2));
// console.log(`These ATBs were missing MICs on at least one accession`, JSON.stringify(Array.from(missingMICs).map(v => `${v[0]}: ${v[1]}`), null, 2));

console.log(`Cumulative Counts by Plate Type:`, JSON.stringify(cumulative_counts_by_plateType, null, 2));
console.log(`${total_samples} Total Samples`);
console.log('done');