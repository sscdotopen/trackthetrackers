package io.ssc.trackthetrackers.analysis

case class ThirdParty(hostDomainIndex: Int, thirdPartyDomain: String, viaScript: Int, viaIFrame: Int,
                      viaImage: Int, viaLink: Int)
