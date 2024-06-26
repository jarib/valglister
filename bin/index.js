const csv = require('csv');
const fs = require('fs');
const path = require('path');
const util = require('util');
const globby = require('globby');

const es = require('elasticsearch');
const ess = require('elasticsearch-streams');
const AgentKeepAlive = require('agentkeepalive');
const transform = require('stream-transform');
const moment = require('moment');

const client = new es.Client({
    host: process.env.ELASTICSEARCH_URL || 'localhost:9200',
    debug: true,
});

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
        await setupIndex();

        const files = await globby(`${__dirname}/../data/*.csv`);

        for (const file of files) {
            console.log(file);

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

            const toBulk = new ess.TransformToBulk((doc) => ({}));

            const parser = csv.parse({
                columns: true,
                delimiter: delimiters[path.basename(file)] || ';',
            });

            await new Promise((resolve, reject) => {
                try {
                    fs.createReadStream(file, 'utf-8')
                        .pipe(parser)
                        .pipe(transform(createTransform(file)))
                        .pipe(toBulk)
                        .pipe(ws)
                        .on('error', reject)
                        .on('finish', resolve);
                } catch (error) {
                    reject(error);
                }
            });
        }

        client.close();
    } catch (error) {
        console.error(error);
        process.exit(1);
    }
})();

async function setupIndex(callback) {
    try {
        await client.indices.delete({ index: 'valglister', ignore: [404] });

        await client.indices.create(
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
            },
            callback
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

        default:
            throw new Error(`don't know how to transform ${file}`);
    }
}
