const parse = require('csv-parse/lib/sync');
const fs = require('fs-extra');
const { groupBy, mapValues, values, keyBy, round } = require('lodash');

(async () => {
    try {
        const data = parse(await fs.readFile(`${__dirname}/../valglister.csv`), {
            columns: true,
        });

        const byKey = mapValues(
            groupBy(data, d => `${d.year}/${d.election}`),
            values =>
                keyBy(
                    values.filter(d => +d.candidateId < 5),
                    d => `${d.name}/${d.yearBorn}`
                )
        );

        let alsoInPrev = 0;
        const keys = Object.keys(byKey['2015/kommunestyre']);

        keys.forEach(key => {
            if (byKey['2011/kommunestyre'][key]) {
                alsoInPrev++;
            }
        });

        const total = keys.length;

        console.log({
            alsoInPrev,
            total,
            percent: round((alsoInPrev * 100) / total),
        });
    } catch (error) {
        console.log(error);
    }
})();
