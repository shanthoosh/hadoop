/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.udf;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.interfaces.UdfResolver;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionBasedUdfResolver implements UdfResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectionBasedUdfResolver.class);

  private static final String CONFIG_PACKAGE_PREFIX = "samza.sql.udf.resolver.package.prefix";

  private final List<UdfMetadata> udfs  = new ArrayList<>();

  public ReflectionBasedUdfResolver(Config udfConfig) {
    // Within Linkedin this value will be set to ["com.linkedin.samza", "org.apache.samza", "com.linkedin.samza.sql.shade.prefix"] through configuration.
    String samzaSqlUdfPackagePrefixes = udfConfig.getOrDefault(CONFIG_PACKAGE_PREFIX, "org.apache.samza");

    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.forPackages(samzaSqlUdfPackagePrefixes.split(","));
    configurationBuilder.addClassLoader(Thread.currentThread().getContextClassLoader());

    Reflections reflections = new Reflections(configurationBuilder);
    Set<Class<?>> typesAnnotatedWithSamzaSqlUdf = reflections.getTypesAnnotatedWith(SamzaSqlUdf.class);

    for (Class<?> udfClass : typesAnnotatedWithSamzaSqlUdf) {
      SamzaSqlUdf sqlUdf = udfClass.getAnnotation(SamzaSqlUdf.class);
      Map<SamzaSqlUdfMethod, Method> udfMethods = new HashMap<>();

      Method[] methods = udfClass.getMethods();
      for (Method method : methods) {
        SamzaSqlUdfMethod sqlUdfMethod = method.getAnnotation(SamzaSqlUdfMethod.class);
        if (sqlUdfMethod != null) {
          udfMethods.put(sqlUdfMethod, method);
        }
      }

      if (udfMethods.isEmpty()) {
        String msg = String.format("UdfClass %s doesn't have any methods annotated with SamzaSqlUdfMethod", udfClass);
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      if (sqlUdf.enabled()) {
        String udfName = sqlUdf.name();
        for (Map.Entry<SamzaSqlUdfMethod, Method> udfMethod : udfMethods.entrySet()) {
          List<SamzaSqlFieldType> params = Arrays.asList(udfMethod.getKey().params());
          LOG.info("Adding the udfName: {}, params: {}, method: {}.", udfName, params, udfMethod.getKey());
          udfs.add(new UdfMetadata(udfName, sqlUdf.description(), udfMethod.getValue(), udfConfig.subset(udfName + "."), params,
              udfMethod.getKey().returns(), udfMethod.getKey().disableArgumentCheck()));
        }
      }
    }
  }

  @Override
  public Collection<UdfMetadata> getUdfs() {
    return udfs;
  }
}
