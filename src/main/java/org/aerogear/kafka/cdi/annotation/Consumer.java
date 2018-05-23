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
package org.aerogear.kafka.cdi.annotation;

import org.aerogear.kafka.DefaultConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Simple annotation for POJO beans, to advertise their one argument methods as a consumer for a given Kafka topic.
 */
@Inherited
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Consumer {
    String[] topics();
    String groupId();
    String offset() default "latest";
    Class<?> keyType() default String.class;
    Class<? extends Deserializer<?>> valueDeserializerClass() default DEFAULT_DESERIALIZER.class;
    Class<? extends ConsumerRebalanceListener> consumerRebalanceListener() default DefaultConsumerRebalanceListener.class;

    static abstract class DEFAULT_DESERIALIZER implements Deserializer<Object> {}
}
