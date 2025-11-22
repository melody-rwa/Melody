// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

contract ArtistRegistry is Initializable, AccessControlUpgradeable, UUPSUpgradeable {
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    struct Artist {
        string profileCid;
        uint8 level;
        bool exists;
    }

    mapping(address => Artist) private artists;
    address[] private allArtists;

    event ArtistRegistered(address indexed artist, string profileCid);
    event ArtistUpdated(address indexed artist, string profileCid);
    event ArtistLevelUpdated(address indexed artist,uint8 level);

    function initialize(address admin,address upgrader) public initializer {
        __AccessControl_init();
        __UUPSUpgradeable_init();
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(UPGRADER_ROLE, upgrader);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    function register(string calldata profileCid) external {
        require(!artists[msg.sender].exists, "already registered");
        artists[msg.sender] = Artist(profileCid, 0,true);
        allArtists.push(msg.sender);
        emit ArtistRegistered(msg.sender, profileCid);
    }

    function update(address artist,string calldata profileCid) external onlyRole(DEFAULT_ADMIN_ROLE){
        require(artists[artist].exists, "not registered");
        artists[artist].profileCid = profileCid;
        emit ArtistUpdated(artist, profileCid);
    }
    function updateLevel(address _artist,uint8 _level) external onlyRole(DEFAULT_ADMIN_ROLE){
        require(artists[_artist].exists, "not registered");
        artists[_artist].level = _level;
        emit ArtistLevelUpdated(_artist, _level);
    }

    function isArtist(address artist) external view returns (bool) {
        return artists[artist].exists;
    }

    function getArtist(address artist) external view returns (Artist memory ) {
        require(artists[artist].exists, "not registered");
        return artists[artist];
    }

    function getArtists(address[] calldata addrs) external view returns (Artist[] memory arts) {
        uint256 len = addrs.length;
        arts = new Artist[](len);
        for (uint256 i = 0; i < len; i++) {
            Artist memory a = artists[addrs[i]];
            require(a.exists, "artist not registered");
            arts[i] = a;
        }
    }

    function totalArtists() external view returns (uint256) {
        return allArtists.length;
    }
}
