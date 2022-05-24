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

package com.intel.oap.expression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.types.DecimalType
object ColumnarExpressionConverter extends Logging {

  var check_if_no_calculation = true

  def replaceWithColumnarExpression(
      expr: Expression,
      attributeSeq: Seq[Attribute] = null,
      expIdx: Int = -1,
      convertBoundRefToAttrRef: Boolean = false): Expression =
    expr match {
      case a: Alias =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        new ColumnarAlias(
          replaceWithColumnarExpression(
            a.child,
            attributeSeq,
            expIdx,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      case a: AttributeReference =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        if (attributeSeq != null) {
          val bindReference =
            BindReferences.bindReference(expr, attributeSeq, true)
          if (bindReference == expr) {
            if (expIdx == -1) {
              return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                a.exprId,
                a.qualifier)
            } else {
              return new ColumnarBoundReference(expIdx, a.dataType, a.nullable)
            }
          }
          val b = bindReference.asInstanceOf[BoundReference]
          new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
        } else {
          return new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            a.qualifier)
        }
      case lit: Literal =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        new ColumnarLiteral(lit)
      case regexp: RegExpReplace =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarRegExpReplaceOperator.create(
          replaceWithColumnarExpression(
            regexp.subject,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            regexp.regexp,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            regexp.rep,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            regexp.pos,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          regexp)

      case binArith: BinaryArithmetic =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryArithmetic.create(
          replaceWithColumnarExpression(
            binArith.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            binArith.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: BoundReference =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        if (convertBoundRefToAttrRef && attributeSeq != null) {
          val a = attributeSeq(b.ordinal)
          new ColumnarAttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
            a.exprId,
            a.qualifier)
        } else {
          new ColumnarBoundReference(b.ordinal, b.dataType, b.nullable)
        }
      case b: BinaryOperator =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: ShiftLeft =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case b: ShiftRight =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case sp: StringPredicate =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            sp.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sp.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case ssi: SubstringIndex =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(
            ssi.strExpr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ssi.delimExpr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ssi.countExpr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case sr: StringRegexExpression =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryOperator.create(
          replaceWithColumnarExpression(
            sr.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sr.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case st: String2TrimExpression =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val exps = st.children.map { expr =>
          replaceWithColumnarExpression(
            expr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef)
        }
        ColumnarString2TrimOperator.create(exps, expr)
      case i: If =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarIfOperator.create(
          replaceWithColumnarExpression(
            i.predicate,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            i.trueValue,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            i.falseValue,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case cw: CaseWhen =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val colBranches = cw.branches.map { expr =>
          {
            (
              replaceWithColumnarExpression(
                expr._1,
                attributeSeq,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef),
              replaceWithColumnarExpression(
                expr._2,
                attributeSeq,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef))
          }
        }
        val colElseValue = cw.elseValue.map { expr =>
          {
            replaceWithColumnarExpression(
              expr,
              attributeSeq,
              convertBoundRefToAttrRef = convertBoundRefToAttrRef)
          }
        }

        logInfo(s"col_branches: $colBranches")
        logInfo(s"col_else: $colElseValue")
        val rename = if (expIdx == -1) false else true
        ColumnarCaseWhenOperator.create(colBranches, colElseValue, expr, rename)
      case c: Coalesce =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val exps = c.children.map { expr =>
          replaceWithColumnarExpression(
            expr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef)
        }
        ColumnarCoalesceOperator.create(exps, expr)
      case i: In =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarInOperator.create(
          replaceWithColumnarExpression(
            i.value,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          i.list,
          expr)
      case i: InSet =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        check_if_no_calculation = false
        ColumnarInSetOperator.create(
          replaceWithColumnarExpression(
            i.child,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          i.hset,
          expr)
      case ss: Substring =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(
            ss.str,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ss.pos,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            ss.len,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case st: StringTranslate =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(st.srcExpr, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(st.matchingExpr, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(st.replaceExpr, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr
        )
      case sl: StringLocate =>
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(sl.substr, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(sl.str, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(sl.start, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr
        )
      case re: RegExpExtract =>
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(re.subject, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(re.regexp, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(re.idx, attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr
        )
      case sr: StringReplace =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarTernaryOperator.create(
          replaceWithColumnarExpression(
            sr.srcExpr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sr.searchExpr,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            sr.replaceExpr,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case u: UnaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        if (!u.isInstanceOf[CheckOverflow] || !u.child.isInstanceOf[Divide]) {
          ColumnarUnaryOperator.create(
            replaceWithColumnarExpression(
              u.child,
              attributeSeq,
              convertBoundRefToAttrRef = convertBoundRefToAttrRef),
            expr)
        } else {
          // CheckOverflow[Divide]: pass resType to Divide to avoid precision loss
          val divide = u.child.asInstanceOf[Divide]
          val columnarDivide = ColumnarBinaryArithmetic.createDivide(
            replaceWithColumnarExpression(
              divide.left,
              attributeSeq,
              convertBoundRefToAttrRef = convertBoundRefToAttrRef),
            replaceWithColumnarExpression(
              divide.right,
              attributeSeq,
              convertBoundRefToAttrRef = convertBoundRefToAttrRef),
            divide,
            u.dataType.asInstanceOf[DecimalType])
          ColumnarUnaryOperator.create(
            columnarDivide,
            expr)
        }
      case c: Concat =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        val exps = c.children.map { expr =>
          replaceWithColumnarExpression(
            expr,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef)
        }
        ColumnarConcatOperator.create(exps, expr)
      case r: Round =>
        check_if_no_calculation = false
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarRoundOperator.create(
          replaceWithColumnarExpression(
            r.child,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            r.scale,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case getArrayItem: GetArrayItem =>
        getArrayItem.child match {
          case strSplit: StringSplit =>
            ColumnarTernaryOperator.create(
              replaceWithColumnarExpression(
                strSplit.str,
                attributeSeq,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef),
              replaceWithColumnarExpression(
                strSplit.regex,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef),
              replaceWithColumnarExpression(
                getArrayItem.ordinal,
                convertBoundRefToAttrRef = convertBoundRefToAttrRef),
              new StringSplit(strSplit.str, strSplit.regex, getArrayItem.ordinal))
          case other =>
            throw new UnsupportedOperationException(
              s" --> ${other.getClass} | ${other} is not currently" +
                s" supported as child of GetArrayItem.")
        }
      case b: BinaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported, no_cal is $check_if_no_calculation.")
        ColumnarBinaryExpression.create(
          replaceWithColumnarExpression(
            b.left,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          replaceWithColumnarExpression(
            b.right,
            attributeSeq,
            convertBoundRefToAttrRef = convertBoundRefToAttrRef),
          expr)
      case expr =>
        throw new UnsupportedOperationException(
          s" --> ${expr.getClass} | ${expr} is not currently supported.")
    }

  def ifNoCalculation = check_if_no_calculation

  def reset(): Unit = {
    check_if_no_calculation = true
  }

  def containsSubquery(expr: Expression): Boolean =
    expr match {
      case a: AttributeReference =>
        return false
      case lit: Literal =>
        return false
      case b: BoundReference =>
        return false
      case u: UnaryExpression =>
        containsSubquery(u.child)
      case b: BinaryOperator =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case i: If =>
        containsSubquery(i.predicate) || containsSubquery(i.trueValue) || containsSubquery(
          i.falseValue)
      case cw: CaseWhen =>
        cw.branches
          .map(p => containsSubquery(p._1) || containsSubquery(p._2))
          .exists(_ == true) || cw.elseValue
          .map(containsSubquery)
          .exists(_ == true)
      case c: Coalesce =>
        c.children.map(containsSubquery).exists(_ == true)
      case i: In =>
        containsSubquery(i.value)
      case ss: Substring =>
        containsSubquery(ss.str) || containsSubquery(ss.pos) || containsSubquery(ss.len)
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        return true
      case c: Concat =>
        c.children.map(containsSubquery).exists(_ == true)
      case getArrayItem: GetArrayItem =>
        getArrayItem.child match {
          case strSplit: StringSplit =>
            strSplit.children.map(containsSubquery).exists(_ == true)
          case other =>
            throw new UnsupportedOperationException(
              s" --> ${other.getClass} | ${other} is not currently" +
                s" supported as child of GetArrayItem.")
        }
      case b: BinaryExpression =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case s: String2TrimExpression =>
        s.children.map(containsSubquery).exists(_ == true)
      case st: StringTranslate =>
        st.children.map(containsSubquery).exists(_ == true)
      case sl: StringLocate =>
        sl.children.map(containsSubquery).exists(_ == true)
      case re: RegExpExtract =>
        re.children.map(containsSubquery).exists(_ == true)
      case regexp: RegExpReplace =>
        containsSubquery(regexp.subject) || containsSubquery(
          regexp.regexp) || containsSubquery(regexp.rep) || containsSubquery(regexp.pos)
      case substrIndex: SubstringIndex =>
        substrIndex.children.map(containsSubquery).exists(_ == true)
      case sr: StringReplace =>
        containsSubquery(sr.srcExpr) ||
          containsSubquery(sr.searchExpr) ||
          containsSubquery(sr.replaceExpr)
      case expr =>
        throw new UnsupportedOperationException(
          s" --> ${expr.getClass} | ${expr} is not currently supported.")
    }

}
