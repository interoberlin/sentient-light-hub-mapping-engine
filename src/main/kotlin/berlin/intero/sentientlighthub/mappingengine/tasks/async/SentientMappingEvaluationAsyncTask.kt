package berlin.intero.sentientlighthub.mappingengine.tasks.async

import berlin.intero.sentientlighthub.common.SentientProperties
import berlin.intero.sentientlighthub.common.model.mapping.Mapping
import berlin.intero.sentientlighthub.common.model.mapping.conditions.AbsoluteThresholdCondition
import berlin.intero.sentientlighthub.common.model.mapping.conditions.DynamicThresholdCondition
import berlin.intero.sentientlighthub.common.tasks.MQTTPublishAsyncTask
import org.springframework.core.task.SimpleAsyncTaskExecutor
import java.util.logging.Logger

/**
 * This async task evaluates a mapping and triggers the mapping's action
 * if the mapping's condition is fulfilled
 *
 * @param mapping to be evaluated
 * @param valuesCurrent current values
 * @param valuesAverage average values
 */
class SentientMappingEvaluationAsyncTask(
        private val mapping: Mapping,
        private val valuesCurrent: Map<String, String>,
        private val valuesAverage: Map<String, String>
) : Runnable {

    companion object {
        private val log: Logger = Logger.getLogger(SentientMappingEvaluationAsyncTask::class.simpleName)
    }

    override fun run() {
        log.info("${SentientProperties.Color.TASK}-- SENTIENT MAPPING EVALUATION TASK${SentientProperties.Color.RESET}")

        val condition = mapping.condition
        val action = mapping.action
        var fulfilled = false

        when (condition) {

            is AbsoluteThresholdCondition -> {
                val checkerboardID = condition.checkerboardID
                val value = valuesCurrent[checkerboardID]

                fulfilled = condition.isFulfilled(checkerboardID, value?.toIntOrNull())
            }

            is DynamicThresholdCondition -> {
                val checkerboardID = condition.checkerboardID
                val averageValue = valuesAverage[checkerboardID]
                val value = valuesCurrent[checkerboardID]

                fulfilled = condition.isFulfilled(checkerboardID, averageValue?.toIntOrNull(), value?.toIntOrNull())

                if (fulfilled) {
                    log.info("${SentientProperties.Color.CONDITION_TRIGGERED} TRIGGERED ${condition.thresholdType} ${SentientProperties.Color.RESET}")
                }
            }
        }

        if (fulfilled) {
            action.apply {
                val topic = "${SentientProperties.MQTT.Topic.LED}/${action.stripID}/${action.ledID}"

                // Call MQTTPublishAsyncTask
                SimpleAsyncTaskExecutor().execute(MQTTPublishAsyncTask(topic, action.value))
            }
        }
    }
}
