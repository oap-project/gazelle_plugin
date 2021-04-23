/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.oap.tpc.h

import java.io.File
import java.util.regex.Pattern

import com.intel.oap.tags.CommentOnContextPR
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.kohsuke.github.GHIssueComment
import org.kohsuke.github.GitHubBuilder
import org.scalatest.FunSuite

class GitHubActionEntries extends FunSuite {

  test("comment on context pr", CommentOnContextPR) {
    def run(): Unit = {
      val enableTPCHTests = Option(System.getenv("ENABLE_TPCH_TESTS"))
      if (!enableTPCHTests.exists(_.toBoolean)) {
        TPCHSuite.stdoutLog("TPCH tests are not enabled, Skipping... ")
        return
      }
      val commentContentPath = System.getenv("COMMENT_CONTENT_PATH")
      if (StringUtils.isEmpty(commentContentPath)) {
        TPCHSuite.stdoutLog("No COMMENT_CONTENT_PATH set. Aborting... ")
        throw new IllegalArgumentException("No COMMENT_CONTENT_PATH set")
      }

      val repoSlug = System.getenv("GITHUB_REPOSITORY")
      println("Reading essential env variables... " +
          "Envs: GITHUB_REPOSITORY: %s" .format(repoSlug))

      if (StringUtils.isEmpty(repoSlug)) {
        throw new IllegalArgumentException("No GITHUB_REPOSITORY set")
      }

      val eventPath = System.getenv("GITHUB_EVENT_PATH")
      println("Reading essential env variables... " +
          "Envs: GITHUB_EVENT_PATH: %s" .format(eventPath))

      if (StringUtils.isEmpty(eventPath)) {
        throw new IllegalArgumentException("No GITHUB_EVENT_PATH set")
      }

      val token = System.getenv("GITHUB_TOKEN")

      if (StringUtils.isEmpty(token)) {
        throw new IllegalArgumentException("No GITHUB_TOKEN set")
      }

      val prUrl = System.getenv("PR_URL")
      val pattern = Pattern.compile("^.*/(\\d+)$")
      val matcher = pattern.matcher(prUrl)
      if (!matcher.matches()) {
        throw new IllegalArgumentException("Unable to find pull request number in URL: " + prUrl)
      }
      val prId = matcher.group(1).toInt

      GitHubActionEntries.commentOnContextPR(repoSlug, prId, token,
        FileUtils.readFileToString(new File(commentContentPath)))
    }
    run()
  }
}

object GitHubActionEntries {
  def commentOnContextPR(repoSlug: String, prId: Int, token: String,
                         comment: String): Option[GHIssueComment] = {
    val inst = new GitHubBuilder()
      .withAppInstallationToken(token)
      .build()

    val repository = inst.getRepository(repoSlug)
    val pr = repository.getPullRequest(prId)
    val c = pr.comment(comment)
    stdoutLog("Comment successfully submitted. ")
    Some(c)
  }

  def stdoutLog(line: Any): Unit = {
    println("[GitHub Action] %s".format(line))
  }
}
