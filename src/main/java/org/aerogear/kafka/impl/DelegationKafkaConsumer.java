/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.impl;

import org.aerogear.kafka.DefaultConsumerRebalanceListener;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class DelegationKafkaConsumer implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DelegationKafkaConsumer.class);

    /*
     * True if a consumer is running; otherwise false
     */
    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);

    private Object consumerInstance;
    final Properties properties = new Properties();
    private KafkaConsumer<?, ?> consumer;
    private List<String> topics;
    private AnnotatedMethod annotatedListenerMethod;
    private ConsumerRebalanceListener consumerRebalanceListener;

    public DelegationKafkaConsumer() {
    }


    private ConsumerRebalanceListener createConsumerRebalanceListener(final Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClazz) {

        if (consumerRebalanceListenerClazz.equals(DefaultConsumerRebalanceListener.class)) {
            return new DefaultConsumerRebalanceListener(consumer);
        } else {
            try {
                return consumerRebalanceListenerClazz.getDeclaredConstructor(org.apache.kafka.clients.consumer.Consumer.class).newInstance(consumer);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                logger.error("Could not create desired listener, using DefaultConsumerRebalanceListener", e);
                return new DefaultConsumerRebalanceListener(consumer);
            }
        }
    }

    private Class<?> consumerKeyType(final Class<?> defaultKeyType, final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length == 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
        } else {
            return defaultKeyType;
        }
    }

    private Class<?> consumerValueType(final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length == 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[1];
        } else {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
        }
    }

    private <K, V> void createKafkaConsumer(final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer, final Properties consumerProperties) {
        consumer = new KafkaConsumer<K, V>(consumerProperties, keyDeserializer, valueDeserializer);
    }


    public void initialize(final String bootstrapServers, final AnnotatedMethod annotatedMethod, final BeanManager beanManager) {
        try {
            final Consumer consumerAnnotation = annotatedMethod.getAnnotation(Consumer.class);
            this.topics = Arrays.asList(consumerAnnotation.topics());
            final String groupId = consumerAnnotation.groupId();
            final Class<?> recordKeyType = consumerAnnotation.keyType();

            this.annotatedListenerMethod = annotatedMethod;

            final Class<?> keyTypeClass = consumerKeyType(recordKeyType, annotatedMethod);
            final Class<?> valTypeClass = consumerValueType(annotatedMethod);

            Class<? extends Deserializer<?>> deserializerValueClass = consumerAnnotation.valueDeserializerClass();
            Deserializer<?> deserializerValue;
            if(deserializerValueClass.equals(Consumer.DEFAULT_DESERIALIZER.class)) {
                deserializerValue = CafdiSerdes.serdeFrom(valTypeClass).deserializer();
                deserializerValueClass = (Class<? extends Deserializer<?>>) deserializerValue.getClass();
            } else {
                deserializerValue = deserializerValueClass.newInstance();
                deserializerValue.configure(new HashMap<String, Class>() {{ put("consumerClass", annotatedMethod.getJavaMember().getParameterTypes()[0]); } } , false);
            }

            properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(GROUP_ID_CONFIG, groupId);
            properties.put(AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset());
            properties.put(KEY_DESERIALIZER_CLASS_CONFIG, CafdiSerdes.serdeFrom(keyTypeClass).deserializer().getClass());
            properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, deserializerValueClass);

            createKafkaConsumer(CafdiSerdes.serdeFrom(keyTypeClass).deserializer(), deserializerValue, properties);
            this.consumerRebalanceListener = createConsumerRebalanceListener(consumerAnnotation.consumerRebalanceListener());

            final Set<Bean<?>> beans = beanManager.getBeans(annotatedListenerMethod.getJavaMember().getDeclaringClass());
            final Bean<?> propertyResolverBean = beanManager.resolve(beans);
            final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);

            consumerInstance = beanManager.getReference(propertyResolverBean,
                    annotatedListenerMethod.getJavaMember().getDeclaringClass(), creationalContext);
        } catch(InstantiationException | IllegalAccessException e) {
            logger.error("Could not instantiate deserializer", e);
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics, consumerRebalanceListener);
            logger.trace("subscribed to {}", topics);
            while (isRunning()) {
                final ConsumerRecords<?, ?> records = consumer.poll(100);
                for (final ConsumerRecord<?, ?> record : records) {
                    try {
                        logger.trace("dispatching payload {} to consumer", record.value());

                        if (annotatedListenerMethod.getJavaMember().getParameterTypes().length == 2) {
                            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value());

                        } else {
                            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.value());
                        }

                        logger.trace("dispatched payload {} to consumer", record.value());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        logger.error("error dispatching received value to consumer", e);
                    }
                }
            }
        } catch (SerializationException e) {
            logger.warn("Consumer exception", e);
            throw e;
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (isRunning()) {
                logger.trace("Exception", e);
                throw e;
            }
        } finally {
            logger.info("Close the consumer.");
            consumer.close();
        }
    }

    /**
     * True when a consumer is running; otherwise false
     */
    public boolean isRunning() {
        return running.get();
    }

    /*
     * Shutdown hook which can be called from a separate thread.
     */
    public void shutdown() {
        logger.info("Shutting down the consumer.");
        running.set(Boolean.FALSE);
        consumer.wakeup();
    }

}
