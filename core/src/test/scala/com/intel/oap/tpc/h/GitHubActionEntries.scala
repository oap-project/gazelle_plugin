package com.intel.oap.tpc.h

import java.io.File

import com.intel.oap.tags.CommentOnContextPR
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.kohsuke.github.{GHIssueComment, GitHubBuilder}
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

      val eventPath = System.getenv("PREVIOUS_EVENT_PATH")
      println("Reading essential env variables... " +
          "Envs: PREVIOUS_EVENT_PATH: %s" .format(eventPath))

      if (StringUtils.isEmpty(eventPath)) {
        throw new IllegalArgumentException("No PREVIOUS_EVENT_PATH set")
      }

      val token = System.getenv("GITHUB_TOKEN")

      if (StringUtils.isEmpty(token)) {
        throw new IllegalArgumentException("No GITHUB_TOKEN set")
      }

      val ghEventPayloadJson = new ObjectMapper().readTree(FileUtils.readFileToString(new File(eventPath)))
      val prId = ghEventPayloadJson.get("number").asInt()

      GitHubActionEntries.commentOnContextPR(repoSlug, prId, token,
        FileUtils.readFileToString(new File(commentContentPath)))
    }
    run()
  }
}

object GitHubActionEntries {
  def commentOnContextPR(repoSlug: String, prId: Int, token: String, comment: String): Option[GHIssueComment] = {
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