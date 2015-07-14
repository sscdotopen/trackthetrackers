package io.ssc.trackthetrackers.analysis

case class ThirdParty(domain: String, thirdPartyDomain: String, numPages: Int, viaScript: Int, viaIFrame: Int,
                      viaImage: Int, viaLink: Int)
