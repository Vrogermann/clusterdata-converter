import { fstat, readdirSync, readFileSync, unlinkSync } from "fs"
import zlib from "zlib"
import cliProgress from "cli-progress"
import colors from 'ansi-colors'
import level, { Level } from "level"
import LineByLine from "n-readlines"
import { writeFileSync } from "fs"
import gc from "expose-gc"

const db = new Level("./database")


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

const taskUsageCsvFields = [
    "start time of the measurement period",
    "end time of the measurement period",
    "job ID",
    "task index",
    "machine ID",
    "mean CPU usage rate",
    "canonical memory usage",
    "assigned memory usage",
    "unmapped page cache memory usage",
    "total page cache memory usage",
    "maximum memory usage",
    "mean disk I/O time",
    "mean local disk space used",
    "maximum CPU usage",
    "maximum disk IO time",
    "cycles per instruction",
    "memory accesses per instruction",
    "sample portion",
    "aggregation type",
    "sampled CPU usage"
]
let verbose = false

async function main(googleTracePath, outputPath, skipJobs, skipTasks) {
    await db.open()
    if (!skipJobs) {
        console.log("Step 1: Processing Job files")
        await readJobFiles(googleTracePath)
        console.log("Step 2: Cleaning up invalid jobs")
        await cleanInvalidJobs()
    }
    if (!skipTasks) {
        console.log("Step 3: Reading Task Files")
        await readTaskFiles(googleTracePath)
    }
    await readTaskUsage(googleTracePath)
    return
}

async function cleanInvalidJobs() {
    const keys = await db.keys().all()
    const jobBar = new cliProgress.SingleBar({
        format: 'Searching for invalid jobs |' + colors.cyan('{bar}') + '| {percentage}% || {value}/{total} || invalid jobs: {invalidJobs}'
    }, cliProgress.Presets.shades_classic);
    jobBar.start(keys.length, 1)
    let invalidJobs = 0

    for (let currentJob = 0; currentJob < keys.length; currentJob++) {
        const data = await db.get(keys[currentJob])
        const job = JSON.parse(data)
        if (!job.finished || job.killed || job.failed) {
            invalidJobs++
            await db.del(keys[currentJob])

        }
        jobBar.update(currentJob + 1, { invalidJobs: invalidJobs })
    }
    jobBar.stop()
}


async function readJobFiles(googleTracePath) {
    let directory = readdirSync(googleTracePath + "job_events")
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, 1)
    for (let currentFile = 0; currentFile < directory.length; currentFile++) {
        fileBar.update(currentFile + 1, { barName: "All Job files progress:" })
        let result = readGoogleTrace(
            googleTracePath,
            "job_events",
            directory[currentFile],
            jobCsvFields,
        )

        const jobBar = multibar.create(result.length, 1)
        for (let currentJob = 0; currentJob < result.length; currentJob++) {
            jobBar.update(currentJob + 1, { barName: "Current Job File (" + currentFile + ") progress:" })
            let job = result[currentJob]
            let jobId = job["job ID"]
            let oldJob;
            try {
                oldJob = JSON.parse(await db.get(jobId))
            } catch (error) {
                oldJob = undefined;
            }

            if (job["event type"] === '0') {
                let newJob = {
                    name: job["logical job name"],
                    schedulingClass: job["schedulingclass"],
                    submitionTime: job["timestamp"],
                    userName: job["user name"],
                    missingInfo: job["missing info"]
                }
                db.put(jobId, JSON.stringify(newJob))
            } else if (oldJob)
                switch (job["event type"]) {
                    // job created
                    // job scheduled
                    case "1":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            scheduleTime: job["timestamp"],
                            executionAttempts: oldJob["executionAttempts"]
                                ? oldJob["executionAttempts"] + 1
                                : 1,
                        }))
                        break
                    // job evicted
                    case "2":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            evictionTime: job["timestamp"],
                            evictionNumber: oldJob["evictionNumber"]
                                ? oldJob["evictionNumber"] + 1
                                : 1,
                        }))
                        break
                    // failure
                    case "3":
                    case "6":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            failureTime: job["timestamp"],
                            failed: true,
                        }))
                        break
                    // finished
                    case "4":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            finishTime: job["timestamp"],
                            finished: true,
                        }))
                        break
                    // killed
                    case "5":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            killedTime: job["timestamp"],
                            killed: true,
                        }))
                        break
                    // updated before scheduling
                    case "7":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            updateTime: job["timestamp"],
                            updatedBeforeSchedule: true,
                        }))
                        break
                    case "8":
                        await db.put(jobId, JSON.stringify({
                            ...oldJob,
                            updateTime: job["timestamp"],
                            updatedBeforeSchedule: false,
                        }))
                        break
                }
        }

        multibar.remove(jobBar)
    }
    fileBar.stop()
}

async function readTaskFiles(googleTracePath) {
    let directory = readdirSync(googleTracePath + "task_events")
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, 0)
    for (let currentFile = 0; currentFile < directory.length; currentFile++) {
        fileBar.update(currentFile, { barName: "All Task files progress:" })
        let result = readGoogleTrace(
            googleTracePath,
            "task_events",
            directory[currentFile],
            taskCsvFields,
        )
        const taskBar = multibar.create(result.length, 0)
        for (let currentEvent = 0; currentEvent < result.length; currentEvent++) {
            taskBar.update(currentEvent, { barName: "Current Task File (" + currentFile + ") progress:" })
            let event = result[currentEvent]
            let jobId = event['job ID']
            let job;
            try {
                job = JSON.parse(await db.get(jobId))
            } catch (error) {
                job = undefined;
            }

            if (job && event['event type'] === '0') {
                const index = Number(event['taskIndex'])
                if (event["CPUcores"]) {
                    job.averageTaskCpuCores = job.averageTaskCpuCores ? (job.averageTaskCpuCores * job.cpuCoresSampleSize + Number(event["CPUcores"])) / (job.cpuCoresSampleSize + 1) : Number(event["CPUcores"])
                    job.cpuCoresSampleSize = job.cpuCoresSampleSize ? job.cpuCoresSampleSize + 1 : 1
                }
                if (event["RAM"]) {
                    job.averageTaskRam = job.averageTaskRam ? (job.averageTaskRam * job.taskRamSampleSize + Number(event["RAM"])) / (job.taskRamSampleSize + 1) : Number(event["RAM"])
                    job.taskRamSampleSize = job.taskRamSampleSize ? job.taskRamSampleSize + 1 : 1
                }
                if (event["diskSpace"]) {
                    job.averageTaskDiskSpace = job.averageTaskDiskSpace ? (job.averageTaskDiskSpace * job.taskDiskSpaceSampleSize + Number(event["diskSpace"])) / (job.taskDiskSpaceSampleSize + 1) : Number(event["diskSpace"])
                    job.taskDiskSpaceSampleSize = job.taskDiskSpaceSampleSize ? job.taskDiskSpaceSampleSize + 1 : 1
                }
                if (!job.numberOfTasks || job.numberOfTasks < index + 1)
                    job.numberOfTasks = index + 1
                db.put(jobId, JSON.stringify(job))
            } else
                if (job) {
                    switch (event["event type"]) {
                        // task evicted
                        case "2":
                            job.taskEvictions = job.taskEvictions ? job.taskEvictions + 1 : 1
                            db.put(jobId, JSON.stringify(job))
                            break

                        // cancelled or missing termination data
                        case "3":
                        case "6":
                            job.taskFailures = job.taskFailures ? job.taskFailures + 1 : 1
                            db.put(jobId, JSON.stringify(job))
                            break
                        // finished
                        case "4":
                            job.tasksCompleted ? job.tasksCompleted + 1 : 1
                            db.put(jobId, JSON.stringify(job))
                            break
                        // killed
                        case "5":
                            job.tasksKilled = job.tasksKilled ? job.tasksKilled + 1 : 1
                            db.put(jobId, JSON.stringify(job))
                            break
                    }
                }
        }
        multibar.remove(taskBar)
    }
    fileBar.stop()
}

async function readTaskUsage(googleTracePath) {
    let directory = readdirSync(googleTracePath + "task_usage")
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, 0)
    for (let currentFile = 19; currentFile < directory.length; currentFile++) {
        fileBar.update(currentFile, { barName: "All Task Usage files progress:" })
        let result = readGoogleTrace(
            googleTracePath,
            "task_usage",
            directory[currentFile],
            taskUsageCsvFields,
        )
        const taskBar = multibar.create(result.length, 0)
        for (let currentUsageReport = 0; currentUsageReport < result.length; currentUsageReport++) {
            taskBar.update(currentUsageReport, { barName: "Current Task Usage File (" + currentFile + ") progress:" })
            let usageReport = result[currentUsageReport]
            let jobId = usageReport['job ID']
            let job;
            try {
                job = JSON.parse(await db.get(jobId))
            } catch (error) {
                job = undefined;
            }

            if (job) {
                job.averageTaskCpuUsage = job.averageTaskCpuUsage ? (job.averageTaskCpuUsage * job.taskCpuUsageSampleSize + Number(usageReport["mean CPU usage rate"])) / (job.taskCpuUsageSampleSize + 1) : Number(usageReport["mean CPU usage rate"])
                job.taskCpuUsageSampleSize = job.taskCpuUsageSampleSize ? job.taskCpuUsageSampleSize + 1 : 1

                job.averageTaskCPI = job.averageTaskCPI ? (job.averageTaskCPI * job.taskCpiSampleSize + Number(usageReport["cycles per instruction (CPI)"])) / (job.taskCpiSampleSize + 1) : Number(usageReport["cycles per instruction (CPI)"])
                job.taskCpiSampleSize = job.taskCpiSampleSize ? job.taskCpiSampleSize + 1 : 1
                db.put(jobId, JSON.stringify(job))
            }
        }
        multibar.remove(taskBar)
    }
    fileBar.stop()
}

function parseCsv(file, headers) {
    let lines = []
    file.split(/\r?\n/).forEach((line, index) => {
        lines.push(parseCsvLine(line, headers))
    });
    return lines
}

function parseCsvLine(line, headers) {
    let item = {}
    line.split(',').forEach((value, index) => {
        item[headers[index]] = value
    })
    return item
}

function readGoogleTrace(basePath, folder, filename, columns) {
    const uncompressedRegex = /.+\.csv(?!\.gz)/
    const compressedRegex = /.+\.csv\.gz(?!\.)/
    global.gc()
    if (compressedRegex.test(filename)) {
        let csv = zlib
            .gunzipSync(readFileSync(basePath + folder + "/" + filename), 'utf-8')

                const extractedFilePath = basePath + folder + "/" + filename + ".extracted.csv"
                writeFileSync(extractedFilePath, csv)
                const liner = new LineByLine(extractedFilePath);
                let currentLine;
                let result = []
                while (currentLine = liner.next()) {
                    result.push(parseCsvLine(currentLine.toString(), columns))
                }
                unlinkSync(extractedFilePath)
                return result


    }
    else if (uncompressedRegex.test(filename)) {
        let csv = readFileSync(basePath + folder + "/" + filename, 'utf-8')
        return parseCsv(csv, columns)
    }
    throw Error(`Unsported file extension on file ${filename}, must be csv or .gz`)
}

let args = process.argv

if (args.length > 3) {
    let googleTracePath = args[2]
    let outputPath = args[3]
    if (args.filter((arg) => arg === "--verbose").length > 0) verbose = true
    let skipJobs = args.filter((arg) => arg === "--skipJobs").length > 0
    let skipTasks = args.filter((arg) => arg === "--skipTasks").length > 0
    main(googleTracePath, outputPath, skipJobs, skipTasks)
} else {
    console.log(
        "Please inform google trace path and desired output path as arguments before running."
    )
}
