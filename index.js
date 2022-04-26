import { readdirSync, readFileSync } from "fs"
import zlib from "zlib"
import { join, dirname } from 'path'
import { Low, JSONFile } from 'lowdb'
import { fileURLToPath } from 'url'
const __dirname = dirname(fileURLToPath(import.meta.url));

// Use JSON file for storage
const file = join(__dirname, 'db.json')
const adapter = new JSONFile(file)
const db = new Low(adapter)
await db.read()
db.data ||= { jobs: [] }

const jobCsvFields = [
    "timestamp",
    "missing info",
    "job ID",
    "event type",
    "user name",
    "schedulingclass",
    "job name",
    "logical job name",
]
const taskCsvFields = [
    "timestamp",
    "missing info",
    "job ID",
    "taskIndex",
    "machine ID",
    "event type",
    "user name",
    "schedulingclass",
    "priority",
    "CPUcores",
    "RAM",
    "diskSpace",
    "different-machine constraint",
]
let verbose = false

async function main(googleTracePath, outputPath) {


    let jobs = readJobFiles(googleTracePath)
    let taskEvents = readTaskFiles(googleTracePath, jobs)
}

function logProgress(name, percentage) {
    console.log(`${name} progress: ${percentage}%`)
}

function log(content) {
    if (verbose) console.log(content)
}

function readJobFiles(googleTracePath) {
    let directory = readdirSync(googleTracePath + "job_events")
    let jobs = new Map()
    for (let currentFile = 0; currentFile < directory.length; currentFile++) {
        let result = readGoogleTrace(
            googleTracePath,
            "job_events",
            directory[currentFile],
            jobCsvFields,
        )
        result.forEach((job) => {
            let jobId = job["job ID"]
            // new job submited
            if (!jobs.has(jobId) && job["event type"] === "0") {
                let newJob = {
                    name: job["logical job name"],
                    schedulingClass: job["schedulingclass"],
                    submitionTime: job["timestamp"],
                    userName: job["user name"],
                    missingInfo: job["missing info"]
                }
                jobs.set(jobId, newJob)
            } else if (jobs.has(jobId)) {

                switch (job["event type"]) {
                    // job scheduled
                    case "1":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            scheduleTime: job["timestamp"],
                            executionAttempts: jobs.get(jobId)["executionAttempts"]
                                ? jobs.get(jobId)["executionAttempts"] + 1
                                : 1,
                        })
                        break
                    // job evicted
                    case "2":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            evictionTime: job["timestamp"],
                            evictionNumber: jobs.get(jobId)["evictionNumber"]
                                ? jobs.get(jobId)["evictionNumber"] + 1
                                : 1,
                        })
                        break
                    // failure
                    case "3":
                    case "6":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            failureTime: job["timestamp"],
                            failed: true,
                        })
                        break
                    // finished
                    case "4":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            finishTime: job["timestamp"],
                            finished: true,
                        })
                        break
                    // killed
                    case "5":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            killedTime: job["timestamp"],
                            killed: true,
                        })
                        break
                    // updated before scheduling
                    case "7":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            updateTime: job["timestamp"],
                            updatedBeforeSchedule: true,
                        })
                        break
                    case "8":
                        jobs.set(jobId, {
                            ...jobs.get(jobId),
                            updateTime: job["timestamp"],
                            updatedBeforeSchedule: false,
                        })
                        break
                }
            }
        })


        logProgress("Job file", Math.floor((currentFile + 1) / directory.length * 100))
    }
    jobs.forEach((job,index) => {
        if(!job.finished && !job.failed && !job.killed){
            jobs.delete(index)
        }
    })
    return jobs
}

function readTaskFiles(googleTracePath, jobs) {
    let directory = readdirSync(googleTracePath + "task_events")
    let taskEvents = []
    for (let currentFile = 0; currentFile < directory.length; currentFile++) {
        let result = readGoogleTrace(
            googleTracePath,
            "task_events",
            directory[currentFile],
            taskCsvFields,
        )
        for (let currentEvent = 0; currentEvent < result.length; currentEvent++) {
            let event = result[currentEvent]
            let jobId = event['job ID']
            if (jobs.has(jobId) && event['event type'] === '0') {
                const job = jobs.get(jobId)
                const index = Number(event['taskIndex'])
                const task = {
                    index: index,
                    schedulingClass: event['schedulingclass'],
                    priority: event['priority'],
                    requestedCpuCores: event["CPUcores"],
                    requestedRam: event["RAM"],
                    requestedDiskSpace: event["diskSpace"],
                    requested: event["different-machine constraint"],
                    submissionTime: event["timestamp"],
                }
                if (!job.tasks) {
                    job.tasks = []
                }
                job.tasks[index] = task
            } else
                if (jobs.has(jobId)) {
                    let job = jobs.get(jobId)
                    let index = Number(event['taskIndex'])
                    switch (event["event type"]) {
                        // task scheduled
                        case "1":

                            const task = {
                                index: index,
                                schedulingClass: event['schedulingclass'],
                                priority: event['priority'],
                                requestedCpuCores: event["CPUcores"],
                                requestedRam: event["RAM"],
                                requestedDiskSpace: event["diskSpace"],
                                requested: event["different-machine constraint"],
                                scheduledTime: event["timestamp"],
                            }
                            if (!job.tasks) {
                                job.tasks = []
                            }
                            job.tasks[index] = { ...job.tasks[index], ...task }
                            break
                        // job evicted
                        case "2":
                            job.tasks[index].evictionNumber = job.tasks[index].evictionNumber ? job.tasks[index].evictionNumber + 1 : 1
                            break
                        // finished
                        case "4":
                            job.tasks[index].finished = true
                            job.tasks[index].finishTime = event['timestamp']
                            break
                        // killed
                        case "5":
                            job.tasks[index].killed = true
                            job.tasks[index].killedTime = event['timestamp']
                            break
                    }
                }
        }
        logProgress("Task file", (currentFile + 1) / directory.length * 100)
    }

}

function parseCsv(file, headers) {
    let lines = []
    file.split(/\r?\n/).forEach((line, index) => {
        const item = {}
        line.split(',').forEach((value, index) => {
            item[headers[index]] = value
        })
        lines.push(item)
    });
    return lines
}

function readGoogleTrace(basePath, folder, filename, columns) {
    const uncompressedRegex = /.+\.csv(?!\.gz)/
    const compressedRegex = /.+\.csv\.gz(?!\.)/
    if (compressedRegex.test(filename)) {
        let csv = zlib
            .gunzipSync(readFileSync(basePath + folder + "/" + filename), 'utf-8').toString()
        return parseCsv(csv, columns)
        // return parse(csv, {
        //     columns: columns,
        //     skip_empty_lines: true,
        // })
    }
    else if (uncompressedRegex.test(filename)) {
        let csv = readFileSync(basePath + folder + "/" + filename, 'utf-8')
        return parseCsv(csv, columns)
        // return parse(csv, {
        //     columns: columns,
        //     skip_empty_lines: true,
        //     })
    }
    throw Error(`Unsported file extension on file ${filename}, must be csv or .gz`)
}

let args = process.argv

if (args.length > 3) {
    let googleTracePath = args[2]
    let outputPath = args[3]
    if (args.filter((arg) => arg === "--verbose").length > 0) verbose = true
    main(googleTracePath, outputPath)
} else {
    console.log(
        "Please inform google trace path and desired output path as arguments before running."
    )
}
