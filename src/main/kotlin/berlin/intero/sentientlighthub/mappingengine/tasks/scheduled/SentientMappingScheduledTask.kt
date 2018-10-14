package berlin.intero.sentientlighthub.mappingengine.tasks.scheduled

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.services.ConfigurationService
import berlin.intero.sentientlighthub.common.tasks.MQTTSubscribeAsyncTask
import berlin.intero.sentientlighthub.mappingengine.tasks.async.SentientMappingEvaluationAsyncTask
import com.google.common.collect.EvictingQueue
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import java.util.logging.Logger

/**
 * This scheduled task
 * <li> calls {@link MQTTSubscribeAsyncTask} to subscribe sensor valuesCurrent from MQTT broker
 * <li> calls {@link SentientMappingEvaluationAsyncTask} for each mapping from configuration
 */
@Component
class SentientMappingScheduledTask {

    val valuesCurrent: MutableMap<String, String> = HashMap()
    val valuesHistoric: MutableMap<String, Queue<String>?> = HashMap()
    val valuesAverage: MutableMap<String, String> = HashMap()

    var valueReceived = false

    companion object {
        private val log: Logger = Logger.getLogger(SentientMappingScheduledTask::class.simpleName)

        private var counter = 0
        private var runtimeMin = Long.MAX_VALUE
        private var runtimeMax = Long.MIN_VALUE
    }

    init {
        val topic = "${SentientProperties.MQTT.Topic.SENSOR}/#"
        val callback = object : MqttCallback {
            override fun messageArrived(topic: String, message: MqttMessage) {
                log.fine("MQTT value received")

                val checkerboardID = topic.split('/').last()
                val value = String(message.payload)

                valueReceived = true

                handleValue(checkerboardID, value)
            }

            override fun connectionLost(cause: Throwable?) {
                log.fine("MQTT connection lost")
            }

            override fun deliveryComplete(token: IMqttDeliveryToken?) {
                log.info("MQTT delivery complete")
            }
        }

        // Call MQTTSubscribeAsyncTask
        val mqttSubscribeAsyncTask = MQTTSubscribeAsyncTask(topic, callback)
        SimpleAsyncTaskExecutor().execute(mqttSubscribeAsyncTask)
    }

    /**
     * Handles received {@param value}
     */
    fun handleValue(checkerboardID: String, value: String) {
        // Add value to current valuesCurrent
        valuesCurrent[checkerboardID] = value

        // Add value to historic valuesCurrent
        var queue = valuesHistoric[checkerboardID]

        if (queue == null) {
            queue = EvictingQueue.create(SentientProperties.Mapping.VALUE_HISTORY)
        }

        queue?.add(value)

        var acc = 0
        queue?.forEach { v -> acc += v.toInt() }
        val avg = acc / queue!!.size

        valuesHistoric[checkerboardID] = queue
        valuesAverage[checkerboardID] = avg.toString()
    }

    @Scheduled(fixedDelay = SentientProperties.Frequency.SENTIENT_MAPPING_DELAY)
    @SuppressWarnings("unused")
    fun map() {
        val startTime = System.currentTimeMillis()
        log.info("${SentientProperties.Color.TASK}-- SENTIENT MAPPING TASK${SentientProperties.Color.RESET}")

        if (valueReceived) {
            valueReceived = false
            ConfigurationService.mappingConfig?.mappings?.forEach { mapping ->

                if (valuesCurrent.isNotEmpty() && valuesAverage.isNotEmpty()) {
                    // Call SentientMappingEvaluationAsyncTask
                    SyncTaskExecutor().execute(SentientMappingEvaluationAsyncTask(mapping, valuesCurrent, valuesAverage))
                }
            }
        } else {
            log.info(".")
            Thread.sleep(SentientProperties.Frequency.UNSUCCESSFUL_TASK_DELAY)
        }

        val endTime = System.currentTimeMillis()
        val runtime = endTime - startTime
        if (counter > 0) {
            if (runtime < runtimeMin) runtimeMin = runtime
            if (runtime > runtimeMax) runtimeMax = runtime
        }

        log.info("-- End counter ${++counter} / ${runtime} millis / min ${runtimeMin} / max ${runtimeMax}")
    }
}
