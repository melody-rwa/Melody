// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

import "@openzeppelin/contracts/proxy/beacon/UpgradeableBeacon.sol";
import "@openzeppelin/contracts/proxy/beacon/BeaconProxy.sol";

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts/utils/Strings.sol";
import "@openzeppelin/contracts/utils/structs/EnumerableMap.sol";

interface IMusicSeries {
    function createSong(
        string calldata ipfsCid,
        uint256 targetBNBWei,
        uint256 durationSec,
        uint256 songId,
        address memeToken,
        uint256 totalSupply
    ) external returns (uint256 tokenId);
}

interface IArtistRegistry {
    function isArtist(address addr) external view returns (bool);
}
interface ILPManager {
    function registerSeries(address series) external;
}
interface IMemeToken {
    function initialize(string calldata name_, string calldata symbol_, address minter, address burner,address admin,address upgrader) external;
    function mint(address to, uint256 amount) external;
}
contract MusicFactory is Initializable, AccessControlUpgradeable, UUPSUpgradeable {
    using Strings for uint256;
    using EnumerableMap for EnumerableMap.Bytes32ToUintMap;

    // ---- Roles ----
    bytes32 public constant UPGRADER_ROLE       = keccak256("UPGRADER_ROLE");
    bytes32 public constant MANAGER_ROLE        = keccak256("MANAGER_ROLE");
    bytes32 public constant INVITE_MANAGER_ROLE = keccak256("INVITE_MANAGER_ROLE");
    bytes32 public constant TARGET_MANAGER_ROLE = keccak256("TARGET_MANAGER_ROLE");
    struct MusicBase {
        address artist;
        string  ipfsCid;
        uint256 targetBNBWei;
        uint256 durationSec;
        string  name;
        string  symbol;
        uint256 time;
        bool confirmed;
        bytes32 salt;
    }
    uint256 public constant TOKENS_PER_SECOND = 1000000;
    uint8   public constant TOKEN_DECIMALS    = 18;
    uint24  public  DEFAULT_FEE_TIER;
    // ---- Beacon & global refs ----
    UpgradeableBeacon public beacon;
    UpgradeableBeacon public beaconMeme;
    address public admin;
    address private upgrader;

    address public lpManager;
    address public artistRegistry;
    address public fractionalVault;
    address public WBNB;
    uint256 public nextSongId;
    uint256 public nextPre;

    // ---- Fundraising targets ----
    uint256[] public allowedTargets;

    // ---- Artists and Series ----
    address[] public allArtists;
    mapping(address => uint256) private artistIndex; // artist → index+1
    mapping(address => address) public artistToSeries;

    // ---- Invites ----
    EnumerableMap.Bytes32ToUintMap private invites; // codeHash -> 1 (valid)
    mapping(uint256 => MusicBase) public musicPre;

    // ---- Events ----
    event BeaconCreated(address indexed beacon, address implementation, address indexed owner);
    event AllowedTargetAdded(uint256 targetWei);
    event AllowedTargetRemoved(uint256 targetWei);
    event ArtistSeriesCreated(address indexed artist, address indexed series, uint256 artistId);
    event InviteAdded(string code);
    event InviteRemoved(string code);
    event InviteConsumed(address indexed artist, string code);
    event MusicCreated(address indexed artist, address indexed series, uint256 tokenId, address memeToken,uint256 songId);
    event MusicPre(address indexed artist,uint256 songPreId,uint256 now,uint256 intervalTime);

    //----- MusicSeries Events ----
    event SongCreated(address indexed seriesAddr,uint256 indexed tokenId, string ipfsCid, uint256 targetBNB, uint256 durationSec, address memeToken,uint256 time,uint256 songId);
    event Subscribed(address indexed seriesAddr,uint256 indexed tokenId, uint256 indexed subId, address user, uint256 amountBNB, uint256 tokenAmount, uint256 secondsBought, uint256 startSec,uint256 songId,uint256 time);
    event Finalized(address indexed seriesAddr,uint256 indexed tokenId, uint256 lpTokenId, uint128 liquidity,uint256 songId);
    event WhitelistAdded(address indexed seriesAddr,uint256 indexed tokenId, address[] accounts,uint256 songId);

    // ---- Initializer ----
    function initialize(
        address _admin,
        address _lpManager,
        address _artistRegistry,
        address _fractionalVault,
        address _WBNB,
        address _musicSeries,
        address _upgrader,
        address _memeTokenAddress,
        uint24 _poolFee
    ) public initializer {
        require(_admin != address(0) && _upgrader!=address(0), "address error");
        __AccessControl_init();
        __UUPSUpgradeable_init();
        admin = _admin;
        upgrader = _upgrader;
        lpManager = _lpManager;
        artistRegistry = _artistRegistry;
        fractionalVault = _fractionalVault;
        WBNB = _WBNB;
        nextSongId = 1;
        nextPre = 1;
        DEFAULT_FEE_TIER = _poolFee;

        _grantRole(DEFAULT_ADMIN_ROLE, _admin);
        _grantRole(UPGRADER_ROLE, _upgrader);
        _grantRole(MANAGER_ROLE, _upgrader);
        _grantRole(INVITE_MANAGER_ROLE, _admin);
        _grantRole(TARGET_MANAGER_ROLE, _admin);

        // deploy MusicSeries implementation and create Beacon
        //address impl = address(new MusicSeries());
        beacon = new UpgradeableBeacon(_musicSeries, _upgrader);
        beaconMeme = new UpgradeableBeacon(_memeTokenAddress, _upgrader);
        // init allowedTargets
        allowedTargets.push(20 ether);
        allowedTargets.push(50 ether);
        allowedTargets.push(100 ether);
        allowedTargets.push(200 ether);

        emit BeaconCreated(address(beacon), _musicSeries, _upgrader);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}
    function getBeacon() external view returns (address) {
        return address(beacon);
    }
    function getBeaconMeme() external view returns (address) {
        return address(beaconMeme);
    }

    // ========== Allowed Targets ==========

    function addAllowedTarget(uint256 targetWei) external onlyRole(TARGET_MANAGER_ROLE) {
        require(targetWei > 0, "target=0");
        require(!isAllowedTarget(targetWei), "exists");
        allowedTargets.push(targetWei);
        emit AllowedTargetAdded(targetWei);
    }

    function removeAllowedTarget(uint256 targetWei) external onlyRole(TARGET_MANAGER_ROLE) {
        uint256 len = allowedTargets.length;
        for (uint256 i = 0; i < len; i++) {
            if (allowedTargets[i] == targetWei) {
                allowedTargets[i] = allowedTargets[len - 1];
                allowedTargets.pop();
                emit AllowedTargetRemoved(targetWei);
                return;
            }
        }
        revert("not found");
    }

    function getAllowedTargets() external view returns (uint256[] memory) {
        return allowedTargets;
    }

    function isAllowedTarget(uint256 targetWei) public view returns (bool) {
        uint256 len = allowedTargets.length;
        for (uint256 i = 0; i < len; i++) {
            if (allowedTargets[i] == targetWei) return true;
        }
        return false;
    }

    // ========== Artist ↔ Series ==========

    function getSeriesOfArtist(address artist) public view returns (address) {
        return artistToSeries[artist];
    }

    function getArtists(uint256 offset, uint256 limit) external view returns (address[] memory artists, address[] memory seriesList) {
        uint256 total = allArtists.length;
        if (offset >= total || limit == 0) {
            return (new address[](0) , new address[](0) );
        }
        uint256 n = limit;
        if (offset + n > total) n = total - offset;

        artists = new address[](n);
        seriesList = new address[](n);
        for (uint256 i = 0; i < n; i++) {
            address art = allArtists[offset + i];
            artists[i] = art;
            seriesList[i] = artistToSeries[art];
        }
    }

    // ========== Invites (simplified) ==========

    function addInvites(string[] calldata codes) external onlyRole(INVITE_MANAGER_ROLE) {
        uint256 n = codes.length;
        require(n > 0, "empty");
        for (uint256 i = 0; i < n; i++) {
            bytes32 h = keccak256(bytes(codes[i]));
            require(!invites.contains(h), "duplicate");
            invites.set(h, 1);
            emit InviteAdded(codes[i]);
        }
    }

    function removeInvite(string calldata code) external onlyRole(INVITE_MANAGER_ROLE) {
        bytes32 h = keccak256(bytes(code));
        require(invites.remove(h), "not found");
        emit InviteRemoved(code);
    }

    function getInvites(uint256 offset, uint256 limit) external view returns (string[] memory codes) {
        uint256 total = invites.length();
        if (offset >= total) return new string[](0) ;
        uint256 n = limit;
        if (offset + n > total) n = total - offset;

        codes = new string[](n);
        for (uint256 i = 0; i < n; i++) {
            (bytes32 h, ) = invites.at(offset + i);
            codes[i] = Strings.toHexString(uint256(h), 32);
        }
    }

    function isInviteValid(string calldata code) public view returns (bool) {
        bytes32 h = keccak256(bytes(code));
        return invites.contains(h);
    }

    // ========== Internal: ensure Series for artist ==========

    function _ensureSeriesForArtist(address artist) internal returns (address series, uint256 artistId) {
        series = artistToSeries[artist];
        if (series != address(0)) {
            return (series, artistIndex[artist]);
        }

        allArtists.push(artist);
        artistIndex[artist] = allArtists.length; // index+1
        artistId = artistIndex[artist];

        string memory collectionName = string(abi.encodePacked("Melody-collection-", artistId.toString()));
        string memory collectionSymbol = string(abi.encodePacked("MEL-collection-", artistId.toString()));

        bytes memory initData = abi.encodeWithSignature(
            "initialize(address,string,string,address,address,uint256,address,address,address,address,uint24)",
            artist,
            collectionName,
            collectionSymbol,
            lpManager,
            admin,
            artistId,
            fractionalVault,
            WBNB,
            address(this),
            upgrader,
            DEFAULT_FEE_TIER
        );

        BeaconProxy proxy = new BeaconProxy(address(beacon), initData);
        series = address(proxy);

        artistToSeries[artist] = series;
        ILPManager(lpManager).registerSeries(series); 
        emit ArtistSeriesCreated(artist, series, artistId);
    }

    // ========== Main: create music (song) ==========

    function createMusic(
        string calldata inviteCode,
        string calldata ipfsCid,
        uint256 targetBNBWei,
        uint256 durationSec,
        string calldata name,
        string calldata symbol,
        uint256 intervalTime,
        bytes32 salt
    ) external returns (address seriesAddr, uint256 tokenId, address memeToken,uint256 songId,uint256 songPreId) {
        if (artistRegistry != address(0)) {
            require(IArtistRegistry(artistRegistry).isArtist(msg.sender), "not artist");
        }
        require(durationSec >= 30, "duration must >=30");
        require(bytes(name).length > 0 && bytes(symbol).length > 0, "name/symbol empty");
        require(salt.length>0,"salt is null");

        bytes32 codeHash = keccak256(bytes(inviteCode));
        require(invites.contains(codeHash), "invalid invite");
        invites.remove(codeHash);
        emit InviteConsumed(msg.sender, inviteCode);

        require(isAllowedTarget(targetBNBWei), "invalid target");

        if(intervalTime>0){
            address predicted = predictAddress(salt);
            require((uint160(predicted) & 0xFFF) == 0x123,"salt predicted error");
            songPreId = nextPre++;
            musicPre[songPreId]= MusicBase({
                artist:msg.sender,
                ipfsCid:ipfsCid,
                targetBNBWei:targetBNBWei,
                durationSec:durationSec,
                name:name,
                symbol:symbol,
                time:block.timestamp+intervalTime,
                confirmed:false,
                salt:salt
            });
            emit MusicPre(msg.sender, songPreId, block.timestamp,intervalTime);
        }else{
            songId = nextSongId++;
            (seriesAddr, ) = _ensureSeriesForArtist(msg.sender);
            (uint256 totalSupply,address memeTokenAddr) = _mintToken(durationSec, name, symbol, seriesAddr,salt);
            memeToken = memeTokenAddr;
            tokenId = IMusicSeries(seriesAddr).createSong(
                ipfsCid,
                targetBNBWei,
                durationSec,
                songId,
                memeToken,
                totalSupply
            );
            emit MusicCreated(msg.sender, seriesAddr, tokenId, memeToken,songId);
        }   
        
        return (seriesAddr, tokenId, memeToken,songId,songPreId);
    }

    function musicConfirm(uint256 songPreId) external returns(address seriesAddr, uint256 tokenId, address memeToken,uint256 songId){
        MusicBase storage music = musicPre[songPreId];
        require(music.artist != address(0),"music not exist");
        require(music.confirmed == false,"music already confirmed");
        require(music.time<=block.timestamp,"the time has not come yet ");
        music.confirmed = true;
        songId = nextSongId++;
        (seriesAddr, ) = _ensureSeriesForArtist(music.artist);
        (uint256 totalSupply,address memeTokenAddr) = _mintToken(music.durationSec, music.name, music.symbol, seriesAddr,music.salt);
        memeToken = memeTokenAddr;
        tokenId = IMusicSeries(seriesAddr).createSong(
            music.ipfsCid,
            music.targetBNBWei,
            music.durationSec,
            songId,
            memeToken,
            totalSupply
        );
        emit MusicCreated(music.artist, seriesAddr, tokenId, memeToken,songId);
    }
    function _mintToken(uint256 _durationSec,string memory _name,string memory _symbol,address _to,bytes32 salt) internal returns(uint256 totalSupply,address memeToken){
        totalSupply = _durationSec * TOKENS_PER_SECOND * (10 ** TOKEN_DECIMALS);
       // MemeToken mem = new MemeToken();
       // mem.initialize(_name, _symbol, address(this),_to, _admin);
       // memeToken = address(mem);
       // mem.mint(_to, totalSupply);
       // bytes memory bytecode = type(MemeToken).creationCode;
       //bytes memory initData = abi.encodeWithSignature(
       //     "initialize(string,string,address,address,address,address)",
       //     _name,
       //     _symbol,
       //     address(this),
       //     _to,
       //     admin,
       //     upgrader
       // );
       bytes memory bytecode = abi.encodePacked(
            type(BeaconProxy).creationCode,
            abi.encode(address(beaconMeme), new bytes(0))
        );
        assembly {
            memeToken := create2(0, add(bytecode, 0x20), mload(bytecode), salt)
            if iszero(extcodesize(memeToken)) {
                revert(0, 0)
            }
        }
        require((uint160(memeToken) & 0xFFF) == 0x123,"salt error");
        IMemeToken(memeToken).initialize(_name, _symbol, address(this),_to, admin,upgrader);
        IMemeToken(memeToken).mint(_to, totalSupply);

    }
    function predictAddress(bytes32 salt) public view returns (address predicted) {
        bytes32 bytecodeHash = getBeaconProxyCodeHash();
        bytes32 data = keccak256(abi.encodePacked(bytes1(0xff), address(this), salt, bytecodeHash));
        predicted = address(uint160(uint256(data)));
    }
    function getBeaconProxyCodeHash() public view returns (bytes32) {
       bytes memory bytecode = abi.encodePacked(
            type(BeaconProxy).creationCode,
            abi.encode(address(beaconMeme), new bytes(0))
        );
        return keccak256(bytecode);
    }

    // ========== Admin setters ==========

    function setLPManager(address _lpManager) external onlyRole(MANAGER_ROLE) { lpManager = _lpManager; }
    function setArtistRegistry(address _artistRegistry) external onlyRole(MANAGER_ROLE) { artistRegistry = _artistRegistry; }
    function setFractionalVault(address _fv) external onlyRole(MANAGER_ROLE) { fractionalVault = _fv; }
    function setWBNB(address _wbnb) external onlyRole(MANAGER_ROLE) { WBNB = _wbnb; }


    function emitSongCreated(address  seriesAddr,uint256  tokenId, string memory ipfsCid, uint256 targetBNB, uint256 durationSec, address memeToken,uint256 time,uint256 songId) public{
        emit SongCreated(seriesAddr,tokenId,ipfsCid,targetBNB,durationSec,memeToken,time,songId);
    }
    function emitSubscribed(address  seriesAddr,uint256  tokenId, uint256  subId, address user, uint256 amountBNB, uint256 tokenAmount, uint256 secondsBought, uint256 startSec,uint256 songId,uint256 time) public {
        emit Subscribed( seriesAddr,tokenId,subId,user,amountBNB, tokenAmount,secondsBought, startSec,songId,time);
    }
    function emitFinalized(address  seriesAddr,uint256  tokenId, uint256 lpTokenId, uint128 liquidity,uint256 songId) public {
        emit Finalized(  seriesAddr,  tokenId,  lpTokenId,  liquidity,songId);
    }
    function emitWhitelistAdded(address  seriesAddr,uint256  tokenId, address[] memory accounts,uint256 songId) public {
        emit WhitelistAdded(  seriesAddr,  tokenId, accounts,songId);
    }
}
