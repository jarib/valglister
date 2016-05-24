const glob = require('glob');
const csv = require('csv');
const fs = require('fs');
const path = require('path');

const es = require('elasticsearch');
const ess = require('elasticsearch-streams');
const AgentKeepAlive = require('agentkeepalive');
const transform = require('stream-transform')

const client = new es.Client({
  host: process.env.ELASTICSEARCH_URL || 'localhost:9200',
  debug: true,
  createNodeAgent(connection, config) {
    return new AgentKeepAlive(connection.makeAgentConfig(config));
  }
});

setupIndex((err) => {
    if (err) {
        throw err;
    }

    glob(`${__dirname}/data/*.csv`, (err, files) => {
        if (err) {
          throw err;
        }

        function readNext() {
            const ws = new ess.WritableBulk((cmds, callback) => {
              client.bulk({
                index: 'valglister',
                type: 'kandidat',
                body: cmds
              }, callback);
            });

            const toBulk = new ess.TransformToBulk(doc => ({}));

            const parser = csv.parse({
                columns: true,
                delimiter: ';'
            });

            const file = files.shift();

            console.log(file)

            fs.createReadStream(file, 'utf-8')
                .pipe(parser)
                .pipe(transform(createTransform(file)))
                .pipe(toBulk)
                .pipe(ws)
                .on('error', console.error)
                .on('finish', finish);
        }

        function finish() {
            files.length ? readNext() : client.close();
        }

        finish();
    });
})

const genders = {
    'M': 'male',
    'K': 'female',
    'Mann': 'male',
    'Kvinne': 'female'
};

function setupIndex(callback) {
    client.indices.delete({index: 'valglister', ignore: [404]})
        .then(() => {
            client.indices.create({
                index: 'valglister',
                type: 'kandidat',
                body: {
                    mappings: {
                        kandidat: {
                            // dynamic_templates: [{
                            //     notanalyzed: {
                            //         match: '*',
                            //         match_mapping_type: 'string',
                            //         mapping: {
                            //             type: 'multi_field',
                            //             fields: {

                            //             }
                            //         }
                            //     }
                            // }],
                            properties: {
                                year: {
                                    type: 'integer',
                                },

                                election: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                countyId: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                countyName: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                municipalityId: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                municipalityName: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                cityDistrict: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                partyId: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                partyName: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                candidateId: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                                name: {
                                    type: 'multi_field',
                                    fields: {
                                        name: { type: 'string' },
                                        raw: { type: 'string', index: 'not_analyzed' },
                                    }
                                },

                                yearBorn: {
                                    type: 'integer'
                                },

                                gender: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },

                            }
                        }
                    }
                }
            }, callback);
        }).catch(console.error)
}

function clean(str) {
    return str.replace(/\s{2,}/g, ' ')
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
                gender: genders[row.KJØNN]
            });
        case 'eksport_kandidater_2011_kommunestyrevalg':
            return (row) => ({
                year: 2011,
                election: 'kommunestyre',
                municipalityId: row.KOMMNR,
                municipalityName: row.KOMMUNE,
                partyId: row.PARTIKODE,
                partyName: row.PARTINAVN,
                candidateId: row.PLASSNR,
                name: clean(row.NAVN),
                yearBorn: row.FØDT,
                gender: genders[row.KJØNN]
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
                gender: genders[row.candidate_gender]

            });
        case 'eksport_kandidater_2015_bydelsutvalg_oslo':
            return (row) => ({
                year: 2015,
                election: 'bydelsutvalg',
                countyId: '0300',
                countyName: 'Oslo',
                cityDistrict: row.Bydel,
                partyId: row.Partikode,
                partyName: row.Parti,
                candidateId: row.Kandidatnr,
                name: clean(row.Kandidat),
                yearBorn: row.Fødselsår,
                gender: genders[row.Kjønn]

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
                gender: genders[row.Kjønn]
            });
        case 'eksport_kandidater_2015_kommunestyrevalg':
            return (row) => ({
                year: 2015,
                election: 'kommunestyre',
                countyName: row.Fylke,
                municipalityId: row.Kommunenr,
                municipalityName: row.Kommune,
                partyId: row.Partikode,
                partyName: row.Parti,
                candidateId: row.Kandidatnr,
                name: clean(row.Navn),
                yearBorn: row.Fødselsår,
                gender: genders[row.Kjønn]

            });
        default:
            throw new Error(`don't know how to transform ${file}`);
    }
}