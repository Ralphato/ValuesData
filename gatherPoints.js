const { InfluxDB } = require('@influxdata/influxdb-client');
const fs = require('fs');

/** Environment variables **/
const url = 'http://localhost:8086'; // change this to http://localhost:8086 if the pc you are using has influxdb set up
const token = "JsPit7YGMRc49iJXz9TGannNogIpwXbV6JfXbmRhV_KcxKbBOmtg8OJY7oU2e78vTkg4BAAYh4q0ElovwepM-g==";
const org = "USA";

// Create InfluxDB client
const influxDB = new InfluxDB({ url, token });
const queryApi = influxDB.getQueryApi(org);

let measurements = [
  'attachdetach_controller_forced_detaches', 'bootstrap_signer_rate_limiter_use', 'container_cpu_cfs_periods_total',
  'container_spec_cpu_period', 'container_spec_cpu_quota', 'container_spec_cpu_shares', 'machine_cpu_cores',
  'node_cpu_core_throttles_total', 'node_cpu_seconds_total', 'node_hwmon_chip_names', 'node_hwmon_temp_max_celsius',
  'process_cpu_seconds_total', 'scheduler_binding_duration_seconds_bucket', 'scheduler_binding_latency_microseconds_sum',
  'scheduler_e2e_scheduling_duration_seconds_bucket', 'scheduler_e2e_scheduling_latency_microseconds_sum',
  'scheduler_scheduling_algorithm_duration_seconds_bucket', 'scheduler_scheduling_latency_seconds_sum'
];

let workBench = ['terasort', 'rf', 'svd', 'wc'];
const labelMap = {
  'terasort1': 1, 'terasort2': 1, 'terasort3': 1, 'terasort4': 1, 'terasort5': 1,
  'rf1': 2, 'rf2': 2, 'rf3': 2, 'rf4': 2, 'rf5': 2,
  'svd1': 3, 'svd2': 3, 'svd3': 3, 'svd4': 3, 'svd5': 3,
  'wc1': 4, 'wc2': 4, 'wc3': 4, 'wc4': 4, 'wc5': 4,
};

const fluxQuery = (i,j, workbench) => `
  from(bucket: "${workbench}${i}/autogen")
  |> range(start: 2019-12-09T01:52:56Z, stop: 2019-12-13T01:52:56Z)
  |> filter(fn: (r) => r["_measurement"] == "${measurements[j]}")
  |> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
  |> group()
  |> yield(name: "mean")
`;

let data = [];
let groupedData = [];
const myQuery = async () => {
  for(let z = 0; z < workBench.length; z++){
    for(let i = 1; i <= 4; i++){
      for (let j = 0; j < measurements.length; j++) {
        const query = fluxQuery(i,j, workBench[z]);
        let index = 1;
        let data2 = {}; // Reset data2 for each measurement
        console.log(`${measurements[j]} starting \n`);
        for await (const { values, tableMeta } of queryApi.iterateRows(query)) {
          const o = tableMeta.toObject(values);
          data2[`value${index}`] = o._value;
          //data2['run'] = i;
          index++;
        }
        groupedData.push({'values' :data2, measurement: measurements[j], workBench: workBench[z] + i, Label: labelMap[workBench[z]+i]});

      }
    }
  }
};

const executeQuery = async () => {
  await myQuery();
  const jsonData = JSON.stringify(groupedData, null, 2);
  //console.log(jsonData);
  //const filtered = jsonData.filter(item => item.measurement === "process_cpu_seconds_total")
  fs.writeFileSync('DataPoints.json', jsonData, 'utf8');
  console.log('Data written successfully');
};

/** Execute a query and receive line table metadata and rows. */
executeQuery();
