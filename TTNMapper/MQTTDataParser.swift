//
//  TTNMQTTDataParser.swift
//  TTNMapper
//
//  Created by Timothy Sealy on 05/08/16.
//  Copyright © 2016 Timothy Sealy. All rights reserved.
//

import Foundation
import CoreLocation

class MQTTDataParser {
    
    fileprivate(set) var ttnbroker : String
    fileprivate(set) var appEUI : String?
    fileprivate(set) var deviceId : String?
    
    init(ttnbroker: String, deviceId: String, appId: String?) {
        self.ttnbroker = ttnbroker
        self.appEUI = appId
        self.deviceId = deviceId
    }
    
    func parseJsonPacket(_ packet: [String : AnyObject]) -> [TTNMapperDatapoint] {
        var ttnmapperPackets = [TTNMapperDatapoint]()
        
        if (ttnbroker == Constants.TTNBROKER_PROD) {
            let parsedPackets = parseProductionJsonPacket(packet)
            
            for parsedPacket in parsedPackets {
                if parsedPacket.isValid() {
                    ttnmapperPackets.append(parsedPacket)
                }
            }
        }
        
        return ttnmapperPackets
    }

    fileprivate func parseProductionJsonPacket(_ packet: [String : AnyObject]) -> [TTNMapperDatapoint] {
        var ttnmapperPackets = [TTNMapperDatapoint]()
        
        let uplink_message = packet["uplink_message"] as? [String: AnyObject]
        if let uplink_message = uplink_message {
            let rx_metadata = uplink_message["rx_metadata"] as? [[String: AnyObject]]
            if let rx_metadata = rx_metadata {
                for rx_metadataItem in rx_metadata {
                    let ttnmapperPacket = TTNMapperDatapoint()
                    ttnmapperPacket.nodeAddr = self.deviceId
                    ttnmapperPacket.appEUI = self.appEUI
                    
                    var gatewayTime = rx_metadataItem["time"] as? String
                    ttnmapperPacket.rssi = rx_metadataItem["rssi"] as? Double
                    ttnmapperPacket.snr = rx_metadataItem["snr"] as? Double
                    
                    ttnmapperPacket.fPort = uplink_message["f_port"] as? Int
                    ttnmapperPacket.fCount = uplink_message["f_cnt"] as? Int
                    
                    var gatewayId: String? = nil
                    let gateway_ids = rx_metadataItem["gateway_ids"] as? [String: AnyObject]
                    if let gateway_ids = gateway_ids {
                        gatewayId = gateway_ids["gateway_id"] as? String
                    }
                    
                    var gatewayAltitude: Double? = nil
                    var gatewayLongitude: Double? = nil
                    var gatewayLatitude: Double? = nil
                    let location = rx_metadataItem["location"] as? [String: AnyObject]
                    if let location = location {
                        gatewayLatitude = location["latitude"] as? Double
                        gatewayLongitude = location["longitude"] as? Double
                        gatewayAltitude = location["altitude"] as? Double
                    }
                    
                    let end_device_ids = packet["end_device_ids"] as? [String: AnyObject]
                    if let end_device_ids = end_device_ids {
                        ttnmapperPacket.nodeAddr = end_device_ids["device_id"] as? String
                        ttnmapperPacket.devEUI = end_device_ids["dev_eui"] as? String
                    }
                    
                    let settings = uplink_message["settings"] as? [String: AnyObject]
                    if let settings = settings {
                        ttnmapperPacket.time = settings["time"] as? String
                        
                        let tempFrequencyString = settings["frequency"] as? String ?? ""
                        let tempFrequencyInt = Int(tempFrequencyString)
                        ttnmapperPacket.frequency = Double(round(1000 * Double(tempFrequencyInt ?? 0) / 1000000)/1000)
                        
                        ttnmapperPacket.codingRate = settings["coding_rate"] as? String ?? ""
                        
                        let data_rate = settings["data_rate"] as? [String: AnyObject]
                        if let data_rate = data_rate {
                            let lora = data_rate["lora"] as? [String: AnyObject]
                            if let lora = lora {
                                ttnmapperPacket.bandwidth = lora["bandwidth"] as? Int
                                ttnmapperPacket.spreadingFactor = lora["spreading_factor"] as? Int
                                let tempBandwidthInt = lora["bandwidth"] as? Int
                                let tempSpreadingFactorInt = lora["spreading_factor"] as? Int
                                let tempBandwidthString = tempBandwidthInt.map(String.init) ?? ""
                                let temptempSpreadingFactorString = tempSpreadingFactorInt.map(String.init) ?? ""
                                ttnmapperPacket.dataRate = "SF" + temptempSpreadingFactorString + "BW" + tempBandwidthString.prefix(3) as String
                            }
                        }
                    }

                    if gatewayTime == nil {
                        gatewayTime = ""
                    }

                    if gatewayAltitude == nil {
                        gatewayAltitude = 0.0
                    }

                    // Use current local time if no timestamp has been provided by the gateway.
                    if gatewayTime == "" {
                        gatewayTime = Date().iso8601
                    }
                    if gatewayId == nil || gatewayId ==  "" {
                        continue
                    }
                    var gatewayLocation: CLLocation?
                    if gatewayLatitude != nil && gatewayLongitude != nil {
                        let gatewayCoordinates = CLLocationCoordinate2D(latitude: gatewayLatitude!, longitude: gatewayLongitude!)
                        gatewayLocation = CLLocation(coordinate: gatewayCoordinates, altitude: gatewayAltitude! as CLLocationDistance, horizontalAccuracy: 0, verticalAccuracy: 0, timestamp: Date())
                    }
                    let gateway = TTNMapperGateway(gatewayId: gatewayId!, timestamp: gatewayTime!, location: gatewayLocation)
                    
                    ttnmapperPacket.gateway = gateway
                    ttnmapperPackets.append(ttnmapperPacket)
                }
            }
        }
        return ttnmapperPackets
    }
    
    fileprivate func parseCroftJsonPacket(_ packet: [String : AnyObject]) -> TTNMapperDatapoint {
        
        // Sample JSON:
        // {"gatewayEui":"00FE34FFFFD30DA7",
        // "nodeEui":"02017201",
        // "time":"2016-06-06T01:12:09.101797367Z",
        // "frequency":868.099975,
        // "dataRate":"SF7BW125",
        // "rssi":-46,
        // "snr":9,
        // "rawData":"QAFyAQIAxAABJeu0TLc=",
        // "data":"IQ=="}
        
        let ttnmapperPacket = TTNMapperDatapoint()
        ttnmapperPacket.nodeAddr = packet["nodeEui"] as? String
        ttnmapperPacket.appEUI = self.appEUI
        ttnmapperPacket.time = packet["time"] as? String
        ttnmapperPacket.frequency = packet["frequency"] as? Double
        ttnmapperPacket.dataRate = packet["dataRate"] as? String
        ttnmapperPacket.rssi = packet["rssi"] as? Double
        ttnmapperPacket.snr = packet["snr"] as? Double
        
        let gatewayAddr = packet["gatewayEui"] as? String
        let gateway = TTNMapperGateway(gatewayId: gatewayAddr!, timestamp: ttnmapperPacket.time!, location: CLLocation())
        ttnmapperPacket.gateway = gateway
        
        return ttnmapperPacket
    }
    
    fileprivate func parseStagingJsonPacket(_ packet: [String : AnyObject]) -> [TTNMapperDatapoint] {
        
        // Sample JSON:
        //["dev_eui": 0000000002017202, "metadata": (
        //{
        //    altitude = 0;
        //    channel = 0;
        //    codingrate = "4/5";
        //    crc = 1;
        //    datarate = SF7BW125;
        //    frequency = "868.1";
        //    "gateway_eui" = 00FE34FFFFD30DA7;
        //    "gateway_timestamp" = 2591438720;
        //    latitude = 0;
        //    longitude = 0;
        //    lsnr = 9;
        //    modulation = LORA;
        //    rfchain = 0;
        //    rssi = "-50";
        //    "server_time" = "2016-06-15T14:12:49.351337424Z";
        //}
        //), "payload": IQ==, "counter": 14729, "port": 1]
        
        var ttnmapperPackets = [TTNMapperDatapoint]()
        
        let nodeAddr = packet["dev_eui"] as? String
        let metadata = packet["metadata"]! as! [[String: AnyObject]]
        
        for item in metadata {
            let ttnmapperPacket = TTNMapperDatapoint()
            ttnmapperPacket.nodeAddr = nodeAddr
            ttnmapperPacket.appEUI = self.appEUI
            ttnmapperPacket.time = item["server_time"] as? String
            ttnmapperPacket.frequency = item["frequency"] as? Double
            ttnmapperPacket.dataRate = item["datarate"] as? String
            ttnmapperPacket.rssi = item["rssi"] as? Double
            ttnmapperPacket.snr = item["lsnr"] as? Double
            
            // Store gateway information.
            let gatewayId = item["gateway_eui"] as? String
            let gatewayLatitude = item["latitude"] as? Double
            let gatewayLongitude = item["longitude"] as? Double
            let gatewayAltitude = item["altitude"] as? Double
            let gatewayCoordinates = CLLocationCoordinate2D(latitude: gatewayLatitude!, longitude: gatewayLongitude!)
            let gatewayLocation = CLLocation(coordinate: gatewayCoordinates, altitude: gatewayAltitude! as CLLocationDistance, horizontalAccuracy: 0, verticalAccuracy: 0, timestamp: Date())
            let gateway = TTNMapperGateway(gatewayId: gatewayId!, timestamp: ttnmapperPacket.time!, location: gatewayLocation)
            ttnmapperPacket.gateway = gateway
            
            ttnmapperPackets.append(ttnmapperPacket)
        }
        
        return ttnmapperPackets
    }
}
