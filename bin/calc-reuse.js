const parse = require('csv-parse/lib/sync');
const fs = require('fs-extra');
const { groupBy, mapValues, values, keyBy } = require('lodash');

(async () => {
    try {
        const data = parse(await fs.readFile(`${__dirname}/../valglister.csv`), {
            columns: true,
        });

        console.log(data[0]);

        const byKey = mapValues(
            groupBy(data, d => `${d.year}/${d.election}`),
            values =>
                keyBy(
                    values.filter(d => +d.cancidateId < 5),
                    d => `${d.name}/${d.yearBorn}`
                )
        );

        let alsoInPrev = 0;

        Object.keys(byKey['2015/kommunestyre']).forEach(key => {
            if (byKey['2011/kommunestyre'][key]) {
                alsoInPrev++;
            }
        });

        console.log({
            alsoInPrev,
            total: Object.keys(byKey['2015/kommunestyre']).length,
        });
    } catch (error) {
        console.log(error);
    }
})();
