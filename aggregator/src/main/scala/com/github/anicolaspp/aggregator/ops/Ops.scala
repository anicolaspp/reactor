package com.github.anicolaspp.aggregator.ops

import com.github.anicolaspp.aggregator.data.Link

object Ops {
  def toCountableLink(link: Link) = (link.path, (1, link))

  def reduceCountableLinks(x: (Int, Link), y: (Int, Link)) = (x._1 + y._1, x._2)

  def toLinkCount(countableLink: (String, (Int, Link))) = countableLink match {
    case (_, (total, link)) => (link, total)
  }
}
