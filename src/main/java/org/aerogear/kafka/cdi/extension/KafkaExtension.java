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
package org.aerogear.kafka.cdi.extension;

import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.impl.DelegationKafkaConsumer;
import org.aerogear.kafka.impl.DelegationStreamProcessor;
import org.aerogear.kafka.impl.InjectedKafkaProducer;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.aerogear.kafka.cdi.annotation.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeShutdown;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.WithAnnotations;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.newSetFromMap;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaExtension<X> implements Extension {

    private String bootstrapServers = null;
    private final Set<AnnotatedMethod<?>> listenerMethods = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<AnnotatedMethod<?>> streamProcessorMethods = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<DelegationKafkaConsumer> managedConsumers = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<org.apache.kafka.clients.producer.Producer> managedProducers = newSetFromMap(new ConcurrentHashMap<>());
    private final Logger logger = LoggerFactory.getLogger(KafkaExtension.class);


    public void kafkaConfig(@Observes @WithAnnotations(KafkaConfig.class) ProcessAnnotatedType pat) {
        logger.trace("Kafka config scanning type: " + pat.getAnnotatedType().getJavaClass().getName());

        final AnnotatedType<X> annotatedType = pat.getAnnotatedType();
        final KafkaConfig kafkaConfig = annotatedType.getAnnotation(KafkaConfig.class);

        // we just do the first
        if (kafkaConfig != null && bootstrapServers == null) {
            logger.info("setting bootstrap.servers IP for, {}", kafkaConfig.bootstrapServers());
            bootstrapServers = VerySimpleEnvironmentResolver.simpleBootstrapServerResolver(kafkaConfig.bootstrapServers());
        }
    }

    public void registerListeners(@Observes @WithAnnotations({Consumer.class, KafkaStream.class}) ProcessAnnotatedType pat) {

        logger.trace("scanning type: " + pat.getAnnotatedType().getJavaClass().getName());
        final AnnotatedType<X> annotatedType = pat.getAnnotatedType();


        for (AnnotatedMethod am : annotatedType.getMethods()) {

            if (am.isAnnotationPresent(Consumer.class)) {

                logger.debug("found annotated listener method, adding for further processing");

                listenerMethods.add(am);
            } else if (am.isAnnotationPresent(KafkaStream.class)) {

                logger.debug("found annotated stream method, adding for further processing");

                streamProcessorMethods.add(am);
            }

        }
    }

    public void afterDeploymentValidation(@Observes AfterDeploymentValidation adv, final BeanManager bm) {

//        final BeanManager bm = CDI.current().getBeanManager();

        logger.debug("wiring annotated methods to internal Kafka Util clazzes");

        listenerMethods.forEach( consumerMethod -> {

            final Bean<DelegationKafkaConsumer> bean = (Bean<DelegationKafkaConsumer>) bm.getBeans(DelegationKafkaConsumer.class).iterator().next();
            final CreationalContext<DelegationKafkaConsumer> ctx = bm.createCreationalContext(bean);
            final DelegationKafkaConsumer frameworkConsumer = (DelegationKafkaConsumer) bm.getReference(bean, DelegationKafkaConsumer.class, ctx);

            // hooking it all together
            frameworkConsumer.initialize(bootstrapServers, consumerMethod, bm);

            managedConsumers.add(frameworkConsumer);
            submitToExecutor(frameworkConsumer);
        });


        streamProcessorMethods.forEach(annotatedStreamMethod -> {
            final Bean<DelegationStreamProcessor> bean = (Bean<DelegationStreamProcessor>) bm.getBeans(DelegationStreamProcessor.class).iterator().next();
            final CreationalContext<DelegationStreamProcessor> ctx = bm.createCreationalContext(bean);
            final DelegationStreamProcessor frameworkProcessor = (DelegationStreamProcessor) bm.getReference(bean, DelegationStreamProcessor.class, ctx);

            frameworkProcessor.init(bootstrapServers, annotatedStreamMethod, bm);
        });
    }

    public void beforeShutdown(@Observes final BeforeShutdown bs) {
        managedConsumers.forEach(delegationKafkaConsumer -> {
            delegationKafkaConsumer.shutdown();
        });

        managedProducers.forEach(managedProducer -> {
            managedProducer.close();
        });
    }

    public <X> void processInjectionTarget(@Observes ProcessInjectionTarget<X> pit) {

        final InjectionTarget<X> it = pit.getInjectionTarget();
        final AnnotatedType<X> at = pit.getAnnotatedType();

        final InjectionTarget<X> wrapped = new InjectionTarget<X>() {
            @Override
            public void inject(X instance, CreationalContext<X> ctx) {
                it.inject(instance, ctx);

                Arrays.asList(at.getJavaClass().getDeclaredFields()).forEach(field -> {
                    final Producer annotation = field.getAnnotation(Producer.class);

                    if (annotation != null) {

                        if (field.getType().isAssignableFrom(SimpleKafkaProducer.class)) {
                            field.setAccessible(Boolean.TRUE);

                            try {
	                            Class<? extends Serializer> valueSerializerClass = annotation.valueSerializer();
	                            Serializer<?> valueSerializer;

	                            final Serde<?> keySerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0]);
	                            if(valueSerializerClass.equals(Producer.DEFAULT_SERIALIZER.class)) {
	                                Serde<?> valSerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[1]);
	                                valueSerializerClass = valSerde.serializer().getClass();
	                                valueSerializer = valSerde.serializer();
	                            } else {
	                                valueSerializer = valueSerializerClass.newInstance();
	                            }

	                            final org.apache.kafka.clients.producer.Producer p = createInjectionProducer(
	                                    bootstrapServers,
	                                    keySerde.serializer().getClass(),
	                                    valueSerializerClass,
	                                    keySerde.serializer(),
	                                    valueSerializer
	                            );

	                            managedProducers.add(p);

	                            try {
	                                field.set(instance, p);
	                            } catch (IllegalArgumentException
	                                    | IllegalAccessException e) {
	                                logger.error("could not inject producer", e);
	                                e.printStackTrace();
	                            }
                            } catch (IllegalAccessException | InstantiationException e) {
                                logger.error("Could not instantiate serializer", e);
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }

            @Override
            public void postConstruct(X instance) {
                it.postConstruct(instance);
            }

            @Override
            public void preDestroy(X instance) {
                it.dispose(instance);
            }

            @Override
            public void dispose(X instance) {
                it.dispose(instance);
            }

            @Override
            public Set<InjectionPoint> getInjectionPoints() {
                return it.getInjectionPoints();
            }

            @Override
            public X produce(CreationalContext<X> ctx) {
                return it.produce(ctx);
            }
        };


        pit.setInjectionTarget(wrapped);
    }

    private void submitToExecutor(final DelegationKafkaConsumer delegationKafkaConsumer) {

        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
        } catch (NamingException e) {
            logger.warn("Could not find a managed ExecutorService, creating one manually");
            executorService = new ThreadPoolExecutor(16, 16, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>());
        }

        // submit the consumer
        executorService.submit(delegationKafkaConsumer);
    }

    private org.apache.kafka.clients.producer.Producer createInjectionProducer(final String bootstrapServers, final Class<?> keySerializerClass, final Class<?> valSerializerClass, final Serializer<?> keySerializer, final Serializer<?> valSerializer ) {

        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, valSerializerClass);

        return new InjectedKafkaProducer(properties, keySerializer, valSerializer);
    }


}
