const csv = require('csv');
const fs = require('fs');
const path = require('path');
const util = require('util');
const globby = require('globby');
const { Transform, Writable } = require('stream');
const { pipeline } = require('stream/promises');

const es = require('elasticsearch');
const ess = require('elasticsearch-streams');
const { transform } = require('stream-transform');
const moment = require('moment');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const CSV_COLUMNS = [
    'year',
    'election',
    'countyId',
    'countyName',
    'county',
    'municipalityId',
    'municipalityName',
    'municipality',
    'cityDistrictId',
    'cityDistrict',
    'partyId',
    'partyName',
    'candidateId',
    'name',
    'residence',
    'yearBorn',
    'dateBorn',
    'gender',
];

const argv = yargs(hideBin(process.argv))
    .option('output', {
        alias: 'o',
        choices: ['es', 'csv', 'both'],
        default: 'es',
        describe: 'Select output target: Elasticsearch, CSV, or both.',
    })
    .option('csv-file', {
        alias: 'f',
        type: 'string',
        default: path.resolve(__dirname, '..', 'valglister.csv'),
        describe: 'Path to write aggregated CSV output when enabled.',
    })
    .help()
    .strict()
    .parse();

const enableEs = argv.output === 'es' || argv.output === 'both';
const enableCsv = argv.output === 'csv' || argv.output === 'both';

const csvOutputPath = path.isAbsolute(argv.csvFile)
    ? argv.csvFile
    : path.resolve(process.cwd(), argv.csvFile);

const client = enableEs
    ? new es.Client({
          host: process.env.ELASTICSEARCH_URL || 'localhost:9200',
          debug: true,
      })
    : null;

const genders = {
    M: 'male',
    K: 'female',
    Mann: 'male',
    Kvinne: 'female',
};

const delimiters = {
    'valglisterogkandidaterstortingsvalget2021.csv': ',',
    'kommunestyrevalget-2023.csv': ',',
    'fylkestingsvalget-2023.csv': ',',
    'bydelsutvalg-2023.csv': ',',
};

!(async () => {
    try {
        if (enableEs) {
            await setupIndex(client);
        }

        const files = await globby(`${__dirname}/../data/*.csv`);
        let csvStringifier;
        let csvOutputStream;
        let csvFinished;

        if (enableCsv) {
            csvStringifier = csv.stringify({
                header: true,
                columns: CSV_COLUMNS,
            });

            csvOutputStream = fs.createWriteStream(csvOutputPath, 'utf-8');
            csvFinished = new Promise((resolve, reject) => {
                csvOutputStream.on('finish', resolve);
                csvOutputStream.on('error', reject);
                csvStringifier.on('error', reject);
            });

            csvStringifier.pipe(csvOutputStream);
        }

        for (const file of files) {
            console.log(file);

            const parser = csv.parse({
                columns: true,
                delimiter: delimiters[path.basename(file)] || ';',
            });

            const csvTap = new Transform({
                objectMode: true,
                transform(doc, enc, callback) {
                    if (enableCsv) {
                        if (!csvStringifier.write(doc)) {
                            csvStringifier.once('drain', () => callback(null, doc));
                            return;
                        }
                    }

                    callback(null, doc);
                },
            });

            const steps = [
                fs.createReadStream(file, 'utf-8'),
                parser,
                transform(createTransform(file)),
                csvTap,
            ];

            if (enableEs) {
                const toBulk = new ess.TransformToBulk(() => ({}));
                const ws = new ess.WritableBulk((cmds, callback) => {
                    client.bulk(
                        {
                            index: 'valglister',
                            type: 'kandidat',
                            body: cmds,
                        },
                        callback
                    );
                });

                steps.push(toBulk, ws);
            } else {
                steps.push(
                    new Writable({
                        objectMode: true,
                        write(_chunk, _encoding, callback) {
                            callback();
                        },
                    })
                );
            }

            await pipeline(...steps);
        }

        if (enableCsv) {
            csvStringifier.end();
            await csvFinished;
        }

        if (enableEs && client) {
            client.close();
        }
    } catch (error) {
        console.error(error);
        process.exit(1);
    }
})();

async function setupIndex(esClient) {
    if (!esClient) {
        return;
    }

    try {
        await esClient.indices.delete({ index: 'valglister', ignore: [404] });

        await esClient.indices.create(
            {
                index: 'valglister',
                body: {
                    mappings: {
                        kandidat: {
                            properties: {
                                year: {
                                    type: 'integer',
                                },

                                election: {
                                    type: 'keyword',
                                },

                                countyId: {
                                    type: 'keyword',
                                },

                                countyName: {
                                    type: 'keyword',
                                },

                                municipalityId: {
                                    type: 'keyword',
                                },

                                municipalityName: {
                                    type: 'keyword',
                                },

                                cityDistrict: {
                                    type: 'keyword',
                                },

                                partyId: {
                                    type: 'keyword',
                                },

                                partyName: {
                                    type: 'keyword',
                                },

                                candidateId: {
                                    type: 'keyword',
                                },

                                name: {
                                    type: 'text',
                                    fields: {
                                        raw: {
                                            type: 'keyword',
                                        },
                                    },
                                },

                                yearBorn: {
                                    type: 'integer',
                                },

                                dateBorn: {
                                    type: 'date',
                                },

                                gender: {
                                    type: 'keyword',
                                },
                            },
                        },
                    },
                },
            }
        );
    } catch (error) {}
}

function clean(str) {
    return str.replace(/\s{2,}/g, ' ');
}

function createTransform(file) {
    switch (path.basename(file, '.csv')) {
        case 'eksport_kandidater_2011_fylkestingsvalg':
            return (row) => ({
                year: 2011,
                election: 'fylkesting',
                countyId: row.KOMMNR,
                countyName: row.KOMMUNE,
                partyId: row.PARTIKODE,
                partyName: row.PARTINAVN,
                candidateId: row.PLASSNR,
                name: clean(row.NAVN),
                yearBorn: row.FØDT,
                gender: genders[row.KJØNN],
            });
        case 'eksport_kandidater_2011_kommunestyrevalg':
            return (row) => ({
                year: 2011,
                election: 'kommunestyre',
                countyId: row.KOMMNR.slice(0, 2),
                municipalityId: row.KOMMNR,
                municipalityName: row.KOMMUNE,
                partyId: row.PARTIKODE,
                partyName: row.PARTINAVN,
                candidateId: row.PLASSNR,
                name: clean(row.NAVN),
                yearBorn: row.FØDT,
                gender: genders[row.KJØNN],
            });
        case 'eksport_kandidater_2013_stortingsvalg':
            return (row) => ({
                year: 2013,
                election: 'storting',
                countyId: row.county_number,
                countyName: row.county,
                partyId: row.party_id,
                partyName: row.party_name,
                candidateId: row.candidate_number,
                name: clean(row.candidate_name),
                yearBorn: row.candidate_birthyear,
                gender: genders[row.candidate_gender],
            });
        case 'eksport_kandidater_2015_bydelsutvalg_oslo':
            return (row) => ({
                year: 2015,
                election: 'bydelsutvalg',
                countyId: '03',
                countyName: 'Oslo',
                cityDistrict: row.Bydel,
                partyId: row.Partikode,
                partyName: row.Parti,
                candidateId: row.Kandidatnr,
                name: clean(row.Kandidat),
                yearBorn: row.Fødselsår,
                gender: genders[row.Kjønn],
            });
        case 'eksport_kandidater_2015_fylkestingsvalg':
            return (row) => ({
                year: 2015,
                election: 'fylkesting',
                countyName: row.Fylke,
                partyId: row.Partikode,
                partyName: row.Parti,
                candidateId: row.Kandidatnr,
                name: clean(row.Kandidat),
                yearBorn: row.Fødselsår,
                gender: genders[row.Kjønn],
            });
        case 'eksport_kandidater_2015_kommunestyrevalg':
            return (row) => ({
                year: 2015,
                election: 'kommunestyre',
                countyId: row.Kommunenr.slice(0, 2),
                countyName: row.Fylke,
                municipalityId: row.Kommunenr,
                municipalityName: row.Kommune,
                partyId: row.Partikode,
                partyName: row.Parti,
                candidateId: row.Kandidatnr,
                name: clean(row.Navn),
                yearBorn: row.Fødselsår,
                gender: genders[row.Kjønn],
            });
        case 'eksport_kandidater_2017_stortingsvalg':
            return (row) => {
                const dateBorn = moment(row.Fødselsdato, 'DD.MM.YYYY');

                return {
                    year: 2017,
                    election: 'storting',
                    countyName: row.Fylke,
                    partyId: row.Partikode,
                    partyName: row.Parti,
                    candidateId: row.Kandidatnr,
                    name: clean(row.Navn),
                    yearBorn: +dateBorn.format('YYYY'),
                    dateBorn: dateBorn.format('YYYY-MM-DD'),
                    gender: genders[row.Kjønn],
                };
            };
        case 'eksport_kandidater2019_fylkestingsvalg':
            return (row) => {
                return {
                    year: 2019,
                    election: 'fylkesting',
                    countyName: row.fylke,
                    partyId: row.partikode,
                    partyName: row.partinavn,
                    candidateId: row.kandidatnr,
                    name: clean(row.navn),
                    yearBorn: row.fødselsår,
                    gender: genders[row.kjønn],
                };
            };
        case 'eksport_kandidater2019_komunestyrevalg':
            return (row) => {
                return {
                    year: 2019,
                    election: 'kommunestyre',
                    municipalityId: row.kommunenr,
                    municipalityName: row.kommune,
                    countyId: row.kommunenr.slice(0, 2),
                    countyName: row.fylke,
                    partyId: row.partikode,
                    partyName: row.partinavn,
                    candidateId: row.kandidatnr,
                    name: clean(row.navn),
                    yearBorn: row.fødselsår,
                    gender: genders[row.kjønn],
                    residence: row.bosted,
                };
            };
        case 'eksport_kandidater2019_valg_bydelsutvalg_oslo':
            return (row) => {
                return {
                    year: 2019,
                    election: 'bydelsutvalg',
                    municipalityId: '0301',
                    municipalityName: 'Oslo',
                    countyId: '03',
                    countyName: 'Oslo',
                    cityDistrict: row.bydel,
                    partyId: row.partikode,
                    partyName: row.partinavn,
                    candidateId: row.kandidatnr,
                    name: clean(row.navn),
                    yearBorn: row.fødselsår,
                    gender: genders[row.kjønn],
                };
            };
        case 'valglisterogkandidaterstortingsvalget2021':
            const counties2021 = {
                Østfold: '01',
                Oslo: '03',
                'Finnmark Finnmárku': '20',
                Akershus: '02',
                'Troms Romsa': '19',
                'Nord-Trøndelag': '17',
                Nordland: '18',
                'Sør-Trøndelag': '16',
                'Sogn og Fjordane': '14',
                'Møre og Romsdal': '15',
                'Vest-Agder': '10',
                'Aust-Agder': '09',
                Rogaland: '11',
                Hordaland: '12',
                Telemark: '08',
                Vestfold: '07',
                Buskerud: '06',
                Oppland: '05',
                Hedmark: '04',
            };

            return (row) => {
                const dateBorn = moment(row.fødselsdato, 'DD.MM.YYYY');
                const countyId = counties2021[row.valgdistrikt];

                if (!countyId) {
                    throw new Error(`unknown county id: ${util.inspect(row)}`);
                }

                return {
                    year: 2021,
                    election: 'storting',
                    countyId,
                    countyName: row.valgdistrikt,
                    partyId: row.partikode,
                    partyName: row.partinavn,
                    candidateId: row.kandidatnr,
                    name: clean(row.navn),
                    yearBorn: +dateBorn.format('YYYY'),
                    dateBorn: dateBorn.format('YYYY-MM-DD'),
                    gender: genders[row.kjønn],
                };
            };
        case 'kommunestyrevalget-2023':
        case 'fylkestingsvalget-2023':
        case 'bydelsutvalg-2023':
            function mapElection(election) {
                if (election === 'Kommunestyrevalget 2023') {
                    return 'kommunestyre';
                }

                if (election === 'Fylkestingsvalget 2023') {
                    return 'fylkesting';
                }

                if (election === 'Valg til bydelsutvalg 2023') {
                    return 'bydelsutvalg';
                }

                throw new Error(`unknown election: ${election}`);
            }

            return (row) => ({
                year: 2023,
                election: mapElection(row.Valg),
                countyId: row.Fylkesnummer,
                county: row.Fylke,
                municipalityId: row.Kommunenummer,
                municipality: row.Kommune,
                cityDistrictId: row.Bydelsnummer,
                cityDistrict: row.Bydel,
                partyName: row.Partinavn,
                candidateId: row.Kandidatnummer,
                name: clean(row.Navn),
                residence: row.Bosted,
                yearBorn: +row.Fødselsår,
                // correct e.g. 2061 to 1961
                dateBorn: row.Fødselsdato.replace(/^\d{4}/, row.Fødselsår),
                gender: genders[row.Kjønn],
            });
        case 'lister_og_kandidater_stortingsvalget_2025':
            // Valg;Valgdistrikt;Partinavn;Kandidatnr;Navn;Bosted;Stilling;Fødselsdato;Alder;Kjønn
            return (row) => ({
                year: 2025,
                election: 'storting',
                countyName: row.Valgdistrikt,
                partyName: row.Partinavn,
                candidateId: row.Kandidatnr,
                name: clean(row.Navn),
                residence: row.Bosted,
                dateBorn: row.Fødselsdato
                    ? moment(row.Fødselsdato, 'DD.MM.YYYY').format('YYYY-MM-DD')
                    : undefined,
                yearBorn: row.Fødselsdato
                    ? +moment(row.Fødselsdato, 'DD.MM.YYYY').format('YYYY')
                    : undefined,
                gender: genders[row.Kjønn],
            });

        default:
            throw new Error(`don't know how to transform ${file}`);
    }
}
