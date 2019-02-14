// jshint esversion: 6
const fs = require('fs');
const path = require('path');

const parse = require('csv-parse/lib/sync');
const stringify = require('csv-stringify/lib/sync');
const argv = require('minimist')(process.argv.slice(2));
const zeroFill = require('zero-fill');
const moment = require('moment');

const lab_name = argv.lab || 'NY - Cornell University Animal Health Diagnostic Center';
const program_name = argv.program || 'NAHLN AMR Pilot Project';
const combined_output_headers = (argv.combined_output_headers || 'Laboratory Name,Unique Specimen ID,State of Animal Origin,Animal Species,Reason for submission ,Program Name,Specimen/ source tissue,Bacterial Organism Isolated,Salmonella Serotype,Final Diagnosis ,Date of Isolation').split(",");
const bacterialOrganismIsolatedColumn = combined_output_headers.indexOf('Bacterial Organism Isolated'); 
const include_header_name = argv.include_header || 'Include';
const input_data_folder = argv.folder || 'C:\\Users\\msp13\\Desktop\\AMRMasterList';
const combined_isolates_filename = argv.combined || `Missy's Master Spreadsheet.csv`;
const sensititre_filename = argv.sensititre || `SWINExportFile.TXT`;

// for name generation
const state = argv.state || 'NY';
const zipcode = argv.zip || '14853';
const unique_name_prefix = `${state}${zipcode}PPY2`;

const combined_isolates_csv = fs.readFileSync(path.join(input_data_folder, combined_isolates_filename), 'utf8');
const sensititre_csv = fs.readFileSync(path.join(input_data_folder, sensititre_filename), 'utf16le').replace(/[\t]+/g, '\t').replace(/[\u0000]+/g, ''); // remove consecutive delimieters
const accession_number_specimen_id_map = {};
const accession_number_date_tested_map = {};

const atb_species_drug_map = {
    'Cattle':  ['AMPICI','CEFTIF','CLINDA','DANOFL','ENROFL','FLORFE','GAMITH','GENTAM','NEOMYC','PENICI','SDIMET','SPECT','TETRA','TIAMUL','TILMIC','TILDIP','TRISUL','TULATH','TYLO'],
    'Swine': ['AMPICI','CEFTIF','CLINDA','DANOFL','ENROFL','FLORFE','GAMITH','GENTAM','NEOMYC','PENICI','SDIMET','SPECT','TETRA','TIAMUL','TILMIC','TILDIP','TRISUL','TULATH','TYLO'],
    'Poultry-domestic chicken': ['AMOXIC','CEFTIF','CLINDA','ENROFL','ERYTH','FLORFE','GENTAM','NEOMYC','NOVOBI','OXYTET','PENICI','SDIMET','SPECT','STREPT','SULTHI','TETRA','TRISUL','TYLO'],
    'Poultry-domestic turkey':  ['AMOXIC','CEFTIF','CLINDA','ENROFL','ERYTH','FLORFE','GENTAM','NEOMYC','NOVOBI','OXYTET','PENICI','SDIMET','SPECT','STREPT','SULTHI','TETRA','TRISUL','TYLO'],
    'Poultry-domestic duck':    ['AMOXIC','CEFTIF','CLINDA','ENROFL','ERYTH','FLORFE','GENTAM','NEOMYC','NOVOBI','OXYTET','PENICI','SDIMET','SPECT','STREPT','SULTHI','TETRA','TRISUL','TYLO'],
    'Equine':  ['AMIKAC','AMPICI','AZITHR','CEFAZO','CEFTAZ','CEFTIF','CHLORA','CLARYT','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','OXACIL','PENICI','RIFAMP','TETRA','TICARC','TICCLA','TRISUL'],
    'Dog':  {
        'dog-cat GN': {
            'drug_map': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEFTAZ','CEPALE','CHLORA','DOXYCY','ENROFL','GENTAM','IMIPEN','MARBOF','ORBIFL','PIPTAZ','PRADOF','TETRA','TRISUL'],
            'organism_regex': /(Escherichia coli)/
        },
        'dog-cat GP': {
            'drug_map': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEPHAL','CHLORA','CLINDA','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','MARBOF','MINOCY','NITRO','OXACIL','PENICI','PRADOF','RIFAMP','TETRA','TRISUL','VANCOM'],
            'organism_regex': /(Staphylococcus)/
        }
    },
    'Cat':  {
        'dog-cat GN': {
            'drug_map': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEFTAZ','CEPALE','CHLORA','DOXYCY','ENROFL','GENTAM','IMIPEN','MARBOF','ORBIFL','PIPTAZ','PRADOF','TETRA','TRISUL'],
            'organism_regex': /(Escherichia coli)/
        },
        'dog-cat GP': {
            'drug_map': ['AMIKAC','AMOCLA','AMPICI','CEFAZO','CEFOVE','CEFPOD','CEPHAL','CHLORA','CLINDA','DOXYCY','ENROFL','ERYTH','GENTAM','IMIPEN','MARBOF','MINOCY','NITRO','OXACIL','PENICI','PRADOF','RIFAMP','TETRA','TRISUL','VANCOM'],
            'organism_regex': /(Staphylococcus)/
        }
    }
};

// pre-process combined isolates data
let combined_isolates_data = parse(combined_isolates_csv, {columns: true});
let starting_number = combined_isolates_data.map(r => r['Unique Specimen ID'].slice(-4))
    .filter(v => v.trim()) // get rid of blanks
    .reduce((t,v) => +v > t ? +v : t, -Infinity); // find the maximum value
if(starting_number === -Infinity){
    starting_number = 0;
}
starting_number++;
console.log(`starting number will be ${zeroFill(4, starting_number)}`);
combined_isolates_data = combined_isolates_data.filter(r => r[include_header_name].toLowerCase() === 'yes');
console.log(`${combined_isolates_data.length} accessions will be included:`);

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
    let date_value = moment(r[39],'YYYY-MM-DD HH:mm:ss').format('M/D/YYYY');
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
    return [date_value].concat(consolidated_drug_data);
});

let allOutputDataRows = combined_isolates_data.map((r, idx) => {
    const accession_number =  r['Accession #']; 
    const bacterialSpecimenIsolated = r['Bacterial Organism Isolated'];
    const salmonellaSerotype = (r['Salmonella Serotype'] || "".trim());
    const isSalmonella = /Salmonella/.test(bacterialSpecimenIsolated);
    if (isSalmonella && !salmonellaSerotype) {
        console.error(`WARNING: Accession #${accession_number} is Salmonella but is missing Serotype`);
        return null;
    }

    accession_number_specimen_id_map[accession_number] = unique_name_prefix + zeroFill(4, starting_number++);
    let row = combined_output_headers.map(h => {            
        switch(h){
        case 'Laboratory Name': return r[h] || lab_name;
        case 'Program Name': return r[h] || program_name;
        case 'Unique Specimen ID': return accession_number_specimen_id_map[accession_number];
        default: return r[h];
        }                
    });    

    let corresponding_sensitire_row = sensititre_data.findIndex(s => s[6] === accession_number); // 6 is 'column G' in the sensititre data    
    if(corresponding_sensitire_row < 0){
        console.error(`Can't find sensititre record for Accesssion #: '${accession_number}'`);
        process.exit(2);
    }
    accession_number_date_tested_map[accession_number] = moment(sensititre_data[corresponding_sensitire_row][39],'YYYY-MM-DD HH:mm:ss').format('M/D/YYYY'); // the test date
    return row.concat(post_sensitire_data[corresponding_sensitire_row]);
});

allOutputDataRows = allOutputDataRows.filter(r => r);

let allOutputDataRowsByAnimalSpecies = {};
const speciesIndex = combined_output_headers.indexOf('Animal Species');
allOutputDataRows.forEach(r => {
    let species = r[speciesIndex];
    if(!allOutputDataRowsByAnimalSpecies[species]){
        allOutputDataRowsByAnimalSpecies[species] = [];
    }
    allOutputDataRowsByAnimalSpecies[species].push(r);
});

console.dir(Object.keys(allOutputDataRowsByAnimalSpecies)
    .map((k) => {
        return {
            species: k,
            count: allOutputDataRowsByAnimalSpecies[k].length
        };
    }) 
);

const atb_offset = combined_output_headers.length + 1;
const dropComplexSpecies = [];

Object.keys(allOutputDataRowsByAnimalSpecies).forEach((species) => {    
    const species_drug_map = atb_species_drug_map[species];    
    const species_has_organism_partition = !Array.isArray(species_drug_map);

    if(species_has_organism_partition){
        const organism_partitions = Object.keys(species_drug_map);
        dropComplexSpecies.push(species);
        organism_partitions.forEach((partition) => {            
            const obj = species_drug_map[partition];            
            const drug_map = obj.drug_map;
            const organism_regex = obj.organism_regex;
            const rows = allOutputDataRowsByAnimalSpecies[species]
              .filter(r => organism_regex.test(r[bacterialOrganismIsolatedColumn])); 
              // r[7] is the Bacterial Organism Isolated
            const meta_species = [species, partition];
            expandSpeciesRows(meta_species, rows, drug_map);
        });
    } else {
        const rows = allOutputDataRowsByAnimalSpecies[species];
        delete allOutputDataRowsByAnimalSpecies[species];
        expandSpeciesRows(species, rows, species_drug_map);
    }
});

dropComplexSpecies.forEach((species) => {
    delete allOutputDataRowsByAnimalSpecies[species];
});

function expandSpeciesRows(species, rows, species_drug_map){
    const num_target_drugs = species_drug_map.length;
    if(num_target_drugs === 0){
        console.log(`Warning: Species '${species}' has no drug map`);
        return;
    }
    const newRows = rows.map((row, idx) => {
        const targetDrugContent = Array(num_target_drugs * 3).fill('');
        for(let i = atb_offset; row[i]; i += 3){
            const a = row[i], b = row[i+1], c = row[i+2];
            const drugIndex = species_drug_map.indexOf(a);
            if(drugIndex < 0){
                const indexOfAccession = Object.keys(accession_number_specimen_id_map).map(k => accession_number_specimen_id_map[k]).indexOf(row[1]);                
                const accessionNumber = Object.keys(accession_number_specimen_id_map)[indexOfAccession];
                console.error(`Encountered unknown drug '${a}' in species '${species}' Sensititre data for Accession # ${accessionNumber}`, i-atb_offset);
                console.log(row.slice(atb_offset));
                process.exit(3);
            }
            const base = drugIndex * 3;
            targetDrugContent[base] = a;
            targetDrugContent[base+1] = b;
            targetDrugContent[base+2] = c;
        }            

        for(let i = 0; i < species_drug_map.length; i++){
            const atb = (targetDrugContent[i*3] || "").trim();
            const mic = (targetDrugContent[i*3 + 1] || "").trim();
            if(species_drug_map[i]){
                const indexOfAccession = Object.keys(accession_number_specimen_id_map).map(k => accession_number_specimen_id_map[k]).indexOf(row[1]);                
                const accessionNumber = Object.keys(accession_number_specimen_id_map)[indexOfAccession];                                
                if(!atb){
                    console.error(`WARNING: Accession # ${accessionNumber} is missing ATB '${species_drug_map[i]}'`);
                } else if(!mic) {
                    console.error(`WARNING: Accession # ${accessionNumber} is missing MIC for ATB '${species_drug_map[i]}'`);
                }
            }


        }

        const newRow = row.slice(0, atb_offset).concat(targetDrugContent).map(v => `"=""${v}"""`);

        return newRow;
    });

    effectiveSpeciesTarget = allOutputDataRowsByAnimalSpecies;
    if(!Array.isArray(species)){
        species = [species];
    }
    while(species.length != 1){
        const s = species.shift();
        effectiveSpeciesTarget = effectiveSpeciesTarget[s];
    }
    species = species.shift();
    if(!allOutputDataRowsByAnimalSpecies[species]){
        allOutputDataRowsByAnimalSpecies[species] = [];
    }
    
    allOutputDataRowsByAnimalSpecies[species] = allOutputDataRowsByAnimalSpecies[species].concat(newRows);
}

console.log('These files will be generated: ');
console.log(Object.keys(allOutputDataRowsByAnimalSpecies).map(k => `${k}.txt`));
Object.keys(allOutputDataRowsByAnimalSpecies).forEach((k) => {
    fs.writeFileSync(path.join(input_data_folder, `${k}.txt`), 
        stringify(allOutputDataRowsByAnimalSpecies[k], {delimiter: '\t', escape: false, quote: false, quotedString: false, quotedEmpty: false }) 
    );
});

console.log(`Back annotating Unique Specimen Id and Date Tested into '${combined_isolates_filename}'`);
combined_isolates_data = parse(combined_isolates_csv, {columns: true});

combined_isolates_data.sort((a, b) => {
    // first sort by Include column
    if (a[include_header_name] && !b[include_header_name]) {
        return -1;
    }
    if (b[include_header_name] && !a[include_header_name]) {
        return 1;
    }
    
    // then sort by Accession # column
    if(a['Accession #'] < b['Accession #']){
        return -1;
    }

    if(a['Accession #'] > b['Accession #']){
        return 1;
    }

    return 0;    
});

let cumulative_counts_by_species = {};
let total_samples = 0;
combined_isolates_data = combined_isolates_data.map(r => {    
    const accession_number = r['Accession #'];
    if(accession_number_specimen_id_map[accession_number] && r['Unique Specimen ID']){
        console.error(`New Unique Id would overwrite existing Unique Id for Accession # ${accession_number}`);
        process.exit(4);
    }

    if(accession_number_date_tested_map[accession_number] && r['Date Tested']){
        console.error(`New Date Tested would overwrite existing Date Tested for Accession # ${accession_number}`);
        process.exit(5);
    }

    r['Unique Specimen ID'] = accession_number_specimen_id_map[accession_number] || r['Unique Specimen ID'];    
    r['Date Tested'] = accession_number_date_tested_map[accession_number] || r['Date Tested'];
    r[include_header_name] = ''; // clear the include header

    // tally cumulative counts by species for those that have unique specimen id's attached
    if(r['Unique Specimen ID']){
        total_samples++;
        let species_organism = `${r['Animal Species']} - `;
        let organism = r['Bacterial Organism Isolated'];
        if(/Salmonella species/.test(organism)){
            species_organism += 'Salmonella species';
        } else if (/Staphylococcus/.test(organism)) {
            species_organism += 'Staphylococcus';
        } else {
            species_organism += organism;
        }

        if(!cumulative_counts_by_species[species_organism]){ cumulative_counts_by_species[species_organism] = 0; }
        cumulative_counts_by_species[species_organism]++;
    }

    return r;
});

fs.writeFileSync(path.join(input_data_folder, 'output.csv'), stringify(combined_isolates_data, {header: true}));

console.log(`Cumulative Counts by Species / Organism:`, JSON.stringify(cumulative_counts_by_species, null, 2));
console.log(`${total_samples} Total Samples`);
console.log('done');