/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonQueryReturnType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JSON path filters on the runtime classpath, which does not get json-smart from the planner.
 *
 * <p>Existence filters initialize jsonpath's {@code DefaultsImpl}; if that fails, only the first
 * existence case reports the root cause, later ones report "Could not initialize class".
 */
class SqlJsonUtilsPathFilterTest {

    private static final String DOC = "[{\"name\":\"a\",\"payload\":\"p\"},{\"name\":\"b\"}]";

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonQueryCases")
    void testJsonQuery(String description, String path, String expected) {
        assertThat(
                        SqlJsonUtils.jsonQuery(
                                DOC,
                                path,
                                JsonQueryReturnType.STRING,
                                JsonQueryWrapper.WITHOUT_ARRAY,
                                JsonQueryOnEmptyOrError.NULL,
                                JsonQueryOnEmptyOrError.ERROR))
                .isEqualTo(expected);
    }

    static Stream<Arguments> jsonQueryCases() {
        return Stream.of(
                Arguments.of("plain path", "$[0]", "{\"name\":\"a\",\"payload\":\"p\"}"),
                Arguments.of("== filter", "$[?(@.name == 'a')].name", "[\"a\"]"),
                Arguments.of("in literal filter", "$[?(@.payload in ['p'])].name", "[\"a\"]"),
                Arguments.of("existence filter", "$[?(@.payload)].name", "[\"a\"]"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonExistsCases")
    void testJsonExists(String description, String path) {
        assertThat(SqlJsonUtils.jsonExists(DOC, path, JsonExistsOnError.ERROR)).isTrue();
    }

    static Stream<Arguments> jsonExistsCases() {
        return Stream.of(
                Arguments.of("plain path", "$[0].payload"),
                Arguments.of("== filter", "$[?(@.name == 'a')]"),
                Arguments.of("in literal filter", "$[?(@.payload in ['p'])]"),
                Arguments.of("existence filter", "$[?(@.payload)]"));
    }
}
