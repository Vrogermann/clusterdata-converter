import { fstat, readdirSync, readFileSync, unlinkSync, appendFileSync, openSync, statSync } from "fs"
import cliProgress from "cli-progress"
import colors from 'ansi-colors'
import level, { Level } from "level"
import LineByLine from "n-readlines"

import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
const db = new Level("./database")
let logFile

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
    "startTime",
    "endTime",
    "jobID",
    "taskIndex",
    "machineID",
    "meanCPU",
    "canonicalMemory",
    "assignedMemory",
    "unmappedCache",
    "totalCache",
    "maxMemory",
    "meanDiskTime",
    "meanDiskSpace",
    "maximumCPU",
    "maximumDiskTime",
    "CPI",
    "MAPI",
    "samplePortion",
    "aggregationType",
    "sampledCPU"
]

async function main(googleTracePath, outputPath, args) {
    if (args.help || !args.tracePath || !args.outputPath) {
        console.log(`
Google Cluster data to Bag-of-Tasks conversion tool
Required arguments:
    --tracePath {location} | specify google cluster data trace location
    --outputPath {location} | specify a folder for the output csv file
Optional arguments:
    --enableLogFile | enable logging to log.txt file
    --skipJobs | skips the job_events processing step
    --skipTasks | skips the task_events processing step
    --skipUsage | skips the task_usage processing step
    --initialUsage {number} | specify the file number from the task_usage to start processing from
    --initialJob {number} | specify the file number from the job_events to start processing from
    --initialTask {number} | specify the file number from the task_events to start processing from
    --maxJobFiles {number} | specify the max number of files from job_events to process
    --maxTaskFiles {number} | specify the max number of files from taskevents to process
    --maxUsageFiles {number} | specify the max number of files from task_usage to process
    --help | shows this message
        `)
        return
    }
    await db.open()
    if (!args.skipJobs) {
        console.log("Step 1: Processing Job files")
        await readJobFiles(googleTracePath, args.initialJob, args.enableLogFile, args.maxJobFiles)
        console.log("Step 2: Cleaning up invalid jobs")
        await cleanInvalidJobs()
    }
    if (!args.skipTasks) {
        console.log("Step 3: Reading Task files")
        await readTaskFiles(googleTracePath, args.initialTask, args.enableLogFile, args.maxTaskFiles)
    }
    if (!args.skipUsage) {
        console.log("Step 4: Reading Task usage files")
        await readTaskUsage(googleTracePath, args.initialUsage, args.enableLogFile, args.maxUsageFiles)
    }
    console.log("Step 5: Writing output File")
    await writeConvertedCsv(outputPath, args.enableLogFile)
    await db.close()
    return
}

function writeToLogFileIfEnabled(message, verbose) {
    if (!verbose) return
    appendFileSync('./log.txt', message + "\n")
}

async function writeConvertedCsv(outputPath) {
    const keys = await db.keys().all()
    const jobBar = new cliProgress.SingleBar({
        format: 'Writing BoT file |' + colors.cyan('{bar}') + '| {percentage}% || {value}/{total}'
    }, cliProgress.Presets.shades_classic);
    jobBar.start(keys.length, 0)
    let line;
    for (let currentJob = 0; currentJob < keys.length; currentJob++) {
        const data = await db.get(keys[currentJob])
        const job = JSON.parse(data)
        //appendFileSync(outputPath + 'converted.csv', line)
        jobBar.update(currentJob + 1)
    }
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


async function readJobFiles(googleTracePath, initialJobFile, maxFiles) {
    writeToLogFileIfEnabled("reading job_events directory", enableLog)
    let directory = readdirSync(googleTracePath + "job_events")
    if (maxFiles)
        directory = directory.slice(0, maxFiles)
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, initialJobFile || 0)
    for (let currentFile = initialJobFile || 0; currentFile < directory.length; currentFile++) {
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

async function readTaskFiles(googleTracePath, initialTaskFile, enableLog, maxFiles) {
    writeToLogFileIfEnabled("reading task_events directory", enableLog)
    let directory = readdirSync(googleTracePath + "task_events")
    if (maxFiles)
        directory = directory.slice(0, maxFiles)
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, initialTaskFile || 0)
    for (let currentFile = initialTaskFile || 0; currentFile < directory.length; currentFile++) {
        writeToLogFileIfEnabled("reading contents of file " + currentFile, enableLog)
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
            const index = Number(event['taskIndex'])
            if (job && event['event type'] === '0') {
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

                    let timestamp
                    switch (event["event type"]) {
                        // task scheduled
                        case "1":

                            timestamp = Number(event["timestamp"])
                            // ignore tasks that started before the trace period (timestamp < 600 seconds)
                            if (timestamp > 600000000) {
                                if (!job.taskTimestamps) {
                                    job.taskTimestamps = {}
                                }
                                job.taskTimestamps[index] = timestamp
                                db.put(jobId, JSON.stringify(job))
                            }
                            break;
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
                            timestamp = Number(event["timestamp"])
                            if (job.taskTimestamps && job.taskTimestamps[index] && timestamp > 600000000) {
                                job.tasksCompleted = job.tasksCompleted ? job.tasksCompleted + 1 : 1
                                let taskDuration = timestamp - job.taskTimestamps[index]
                                job.averageTaskDuration = job.averageTaskDuration ? (job.averageTaskDuration * job.taskDurationSampleSize + taskDuration) / (job.taskDurationSampleSize + 1) : taskDuration
                                job.taskDurationSampleSize = job.taskDurationSampleSize ? job.taskDurationSampleSize + 1 : 1
                                delete job.taskTimestamps[index]
                            }
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
    return
}

async function readTaskUsage(googleTracePath, initialUsageFile, enableLog, maxFiles) {
    writeToLogFileIfEnabled("reading task_sage directory", enableLog)
    let directory = readdirSync(googleTracePath + "task_usage")
    if (maxFiles)
        directory = directory.slice(0, maxFiles)
    const multibar = new cliProgress.MultiBar({
        clearOnComplete: false,
        hideCursor: true,
        format: '{barName} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'

    }, cliProgress.Presets.shades_grey);
    const fileBar = multibar.create(directory.length, initialUsageFile || 0, { barName: "All Task Usage files progress:" })
    for (let currentFile = initialUsageFile || 0; currentFile < directory.length; currentFile++) {

        writeToLogFileIfEnabled("reading contents of file " + currentFile, enableLog)
        let generator = lineGenerator(
            googleTracePath,
            "task_usage",
            directory[currentFile],
            taskUsageCsvFields,
        )
        const taskBar = multibar.create(getFileSize(googleTracePath, "task_usage", directory[currentFile]),
            0,
            { barName: "Current Task File (" + directory[currentFile] + ") progress:" })

        let nextLine = generator.next()
        while (nextLine.value?.content) {
            let usageReport = nextLine.value.content

            let jobId = usageReport['jobID']
            let job;
            try {
                job = JSON.parse(await db.get(jobId))
            } catch (error) {
                job = undefined;
            }

            if (job) {
                if (usageReport["meanCPU"]) {
                    job.averageTaskCpuUsage = job.averageTaskCpuUsage ? (job.averageTaskCpuUsage * job.taskCpuUsageSampleSize + Number(usageReport["meanCPU"])) / (job.taskCpuUsageSampleSize + 1) : Number(usageReport["meanCPU"])
                    job.taskCpuUsageSampleSize = job.taskCpuUsageSampleSize ? job.taskCpuUsageSampleSize + 1 : 1
                }
                if (usageReport["CPI"]) {
                    job.averageTaskCPI = job.averageTaskCPI ? (job.averageTaskCPI * job.taskCpiSampleSize + Number(usageReport["CPI"])) / (job.taskCpiSampleSize + 1) : Number(usageReport["CPI"])
                    job.taskCpiSampleSize = job.taskCpiSampleSize ? job.taskCpiSampleSize + 1 : 1
                }

                db.put(jobId, JSON.stringify(job))
            }
            taskBar.increment(nextLine.value.size)
            nextLine = generator.next()
        }
        multibar.remove(taskBar)
        fileBar.increment()
    }
    fileBar.stop()
}


function parseCsvLine(line, headers) {
    let item = {}
    let info = line.split(',')
    for (let currentInfo = 0; currentInfo < info.length; currentInfo++)

        item[headers[currentInfo]] = info[currentInfo]
    return item
}

function readGoogleTrace(basePath, folder, filename, columns) {
    const uncompressedRegex = /.+\.csv(?!\.gz)/
    const compressedRegex = /.+\.csv\.gz(?!\.)/
    if (uncompressedRegex.test(filename)) {

        const liner = new LineByLine(basePath + folder + "/" + filename);
        let currentLine;
        let result = []
        while (currentLine = liner.next()) {
            result.push(parseCsvLine(currentLine.toString(), columns))
        }
        return result
    }
    console.log(`Unsported file extension on file ${filename}, must be .csv `)
    return [];
}

function* lineGenerator(basePath, folder, filename, columns) {
    const uncompressedRegex = /.+\.csv(?!\.gz)/
    if (uncompressedRegex.test(filename)) {

        const liner = new LineByLine(basePath + folder + "/" + filename);
        let currentLine;
        while (currentLine = liner.next()) {
            yield {
                content: parseCsvLine(currentLine.toString(), columns),
                size: currentLine.byteLength
            }
        }
        return
    }
    console.log(`Unsported file extension on file ${filename}, must be .csv `)
    return;
}

function getFileSize(basePath, folder, filename) {
    const fileDescriptor = statSync(basePath + folder + "/" + filename)
    return fileDescriptor.size

}

let args = yargs(hideBin(process.argv)).argv
main(args.tracePath, args.outputPath, args)


