/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.extraction.resources;

import com.google.javascript.jscomp.parsing.Config;
import com.google.javascript.jscomp.parsing.ParserRunner;
import com.google.javascript.rhino.ErrorReporter;
import com.google.javascript.rhino.jstype.SimpleSourceFile;
import com.google.javascript.rhino.jstype.StaticSourceFile;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JavascriptParser {

  private final Config config = ParserRunner.createConfig(true, Config.LanguageMode.ECMASCRIPT5_STRICT, true,
      EXTRA_ANNOTATIONS);
  private final StaticSourceFile sourceFile = new SimpleSourceFile("input", false);

  private final ErrorReporter errorReporter = new ErrorReporter() {
    @Override
    public void warning(String message, String sourceName, int line, int lineOffset) {}

    @Override
    public void error(String message, String sourceName, int line, int lineOffset) {}
  };

  private static final Set<String> EXTRA_ANNOTATIONS =
      new HashSet<String>(Arrays.asList("suppressReceiverCheck", "suppressGlobalPropertiesCheck"));

  public ParserRunner.ParseResult parse(String script) {
    return ParserRunner.parse(sourceFile, script, config, errorReporter);
  }

}
