// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

/*
MusicSeries.sol (optimized with EnumerableMap)

Key changes in this version:
- contributors[], contributorAdded, and mapping(address=>uint256) contribution replaced with EnumerableMap.AddressToUintMap
- subscribe() accumulates contributions via EnumerableMap
- finalize() iterates EnumerableMap directly to build user & contribution arrays
*/

import "@openzeppelin/contracts-upgradeable/token/ERC721/ERC721Upgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/ReentrancyGuardUpgradeable.sol";
import "@openzeppelin/contracts/utils/Strings.sol";
import "@openzeppelin/contracts/utils/structs/EnumerableMap.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import "./MemeToken.sol";

interface ILPManagerV3 {
    function addLiquidityAndFinalizeV3(
        uint256 songId,
        address token0,
        address token1,
        uint24 fee,
        int24 tickLower,
        int24 tickUpper,
        uint256 amount0Desired,
        uint256 amount1Desired,
        uint256 amount0Min,
        uint256 amount1Min,
        address artist
    ) external payable returns (uint256 lpTokenId, uint128 liquidity);
}

interface IMusicFactory{
    function emitSongCreated(address  seriesAddr,uint256  tokenId, string memory ipfsCid, uint256 targetBNB, uint256 durationSec, address memeToken,uint256 time,uint256 songId) external;
    function emitSubscribed(address  seriesAddr,uint256  tokenId, uint256  subId, address user, uint256 amountBNB, uint256 tokenAmount, uint256 secondsBought, uint256 startSec,uint256 songId,uint256 time) external;
    function emitFinalized(address  seriesAddr,uint256  tokenId, uint256 lpTokenId, uint128 liquidity,uint256 songId) external;
    function emitWhitelistAdded(address  seriesAddr,uint256  tokenId, address[] memory accounts,uint256 songId) external;
}
interface IWBNB is IERC20 {
    function deposit() external payable;   // wrap BNB → WBNB
    function withdraw(uint256) external;   // unwrap WBNB → BNB
}
contract MusicSeries is Initializable, ERC721Upgradeable, UUPSUpgradeable, AccessControlUpgradeable, PausableUpgradeable, ReentrancyGuardUpgradeable {
    using Strings for uint256;
    using EnumerableMap for EnumerableMap.AddressToUintMap;

    bytes32 public constant MANAGER_ROLE  = keccak256("MANAGER_ROLE");
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");
    bytes32 public constant ARTISTER_ROLE = keccak256("ARTISTER_ROLE");

    uint256 public constant TOKENS_PER_SECOND = 1000000;
    uint8   public constant TOKEN_DECIMALS    = 18;
    uint256 public constant DURATION_ENLARGE = 10000;

    int24   public constant MIN_TICK = -887272;
    int24   public constant MAX_TICK =  887272;
    address public  WBNB ; 
    uint24  public  DEFAULT_FEE_TIER;
    address public musicFactory;


    enum Phase { Collecting, Whitelist, Interval, Public, Finalized }

    struct Subscription {
        uint256 id;
        address user;
        uint256 amountBNB;
        uint256 startSec;
        uint256 secondsBought;
        uint256 tokenAmount;
        uint256 timestamp;
    }

    struct Song {
        uint256 songId;
        uint256 tokenId;
        string ipfsCid;
        uint256 targetBNB;
        uint256 durationSec;
        address memeToken;
        uint256 totalMemeSupply;
        uint256 raisedBNB;
        uint256 subscribedTokens;
        uint256 lpTokenId;      // V3 NFT LP tokenId
        uint128 lpLiquidity;    // V3 liquidity
        uint256 nextSubId;
        uint256 nextAvailableSecond;

        uint256 createdAt;
        bool finalized;

        // V3 config
        address baseToken;
        uint24 feeTier;
        int24 tickLower;
        int24 tickUpper;

        mapping(uint256 => Subscription) subs;
        EnumerableMap.AddressToUintMap contribution;
        EnumerableMap.AddressToUintMap wlMap;
    }



    mapping(uint256 => Song) private songs;
    uint256[] private allSongIds;
    uint256 public nextTokenId;

    address public artist;
    uint256 public artistId;
    address public lpManager;
    address public fractionalVault;
    address public admin;

    function initialize(
        address _artist,
        string calldata collectionName,
        string calldata collectionSymbol,
        address _lpManager,
        address _admin,
        uint256 _artistId,
        address _fractionalVault,
        address _WBNB,
        address _musicFactory,
        address _upgrader,
        uint24 _poolfee
    ) public initializer {
        __ERC721_init(collectionName, collectionSymbol);
        __AccessControl_init();
        __UUPSUpgradeable_init();
        __Pausable_init();
        __ReentrancyGuard_init();

        _grantRole(DEFAULT_ADMIN_ROLE, _admin);
        _grantRole(MANAGER_ROLE, _musicFactory);
        _grantRole(UPGRADER_ROLE, _upgrader);
        _grantRole(ARTISTER_ROLE, _artist);

        artist = _artist;
        lpManager = _lpManager;
        fractionalVault = _fractionalVault;
        admin = _admin;
        artistId = _artistId;
        nextTokenId = 1;
        DEFAULT_FEE_TIER = _poolfee;
        WBNB = _WBNB;
        musicFactory = _musicFactory;
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    function currentPhase(uint256 songId) public view returns (Phase) {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");

        if (s.finalized) return Phase.Finalized;

        uint256 t = block.timestamp;
        uint256 start = s.createdAt;

        //if (t < start + 1 days) return Phase.Collecting;
        //if (t < start + 3 days) return Phase.Whitelist;
        //if (t < start + 4 days) return Phase.Interval;
        //if (t < start + 6 days) return Phase.Public;

        if (t < start + 2 minutes) return Phase.Collecting;
        if (t < start + 4 minutes) return Phase.Whitelist;
        if (t < start + 6 minutes) return Phase.Interval;
        if (t < start + 8 minutes) return Phase.Public;

        return Phase.Finalized;
    }

function createSong(
    string calldata ipfsCid,
    uint256 targetBNBWei,
    uint256 durationSec,
    uint256 songId,
    address memeTokenAddr,
    uint256 totalSupply
) external onlyRole(MANAGER_ROLE) returns (uint256 tokenId) {
    require(targetBNBWei > 0, "target required");
    require(memeTokenAddr!=address(0),"memeToken address is null");
    tokenId = nextTokenId++;
    _safeMint(artist, tokenId);
    uint256 durationEnlarge = DURATION_ENLARGE * durationSec;
    Song storage s = songs[songId];
    s.ipfsCid = ipfsCid;
    s.targetBNB = targetBNBWei;
    s.durationSec = durationEnlarge;
    s.memeToken = memeTokenAddr;
    s.totalMemeSupply = totalSupply;
    s.createdAt = block.timestamp;
    s.nextSubId = 1;
    s.raisedBNB = 0;
    s.subscribedTokens = 0;
    s.finalized = false;
    s.songId = songId;
    s.tokenId = tokenId;
    s.baseToken = WBNB;
    s.feeTier   = DEFAULT_FEE_TIER;
    s.tickLower = MIN_TICK;
    s.tickUpper = MAX_TICK;

    allSongIds.push(songId);

    IMusicFactory(musicFactory).emitSongCreated(address(this),tokenId, ipfsCid, targetBNBWei, durationEnlarge, memeTokenAddr,block.timestamp,songId);
    return tokenId;
}


    // Add whitelist addresses
    function addWhitelist(uint256 songId, address[] calldata accounts) external onlyRole(ARTISTER_ROLE) {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");
        Phase p = currentPhase(songId);
        require(p == Phase.Collecting, "not collecting");
        uint256 wlMaxSlots = s.targetBNB / 0.1 ether;
        require(s.wlMap.length() + accounts.length <= wlMaxSlots, "exceed wl slots");

        for (uint256 i = 0; i < accounts.length; i++) {
            require(!s.wlMap.contains(accounts[i]), "duplicate");
            s.wlMap.set(accounts[i], 0);
        }

        IMusicFactory(musicFactory).emitWhitelistAdded(address(this),s.tokenId, accounts,songId);
    }
    // ===== WhiteList Queries =====

    
    function isWhitelisted(uint256 songId, address user) external view returns (bool) {
        return songs[songId].wlMap.contains(user);
    }

    function getWhitelistSpent(uint256 songId, address user) external view returns (uint256) {
        if (!songs[songId].wlMap.contains(user)) return 0;
        return songs[songId].wlMap.get(user);
    }

    // Subscribe
    function subscribe(uint256 songId) external payable whenNotPaused nonReentrant {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");

        Phase p = currentPhase(songId);
        require(p == Phase.Whitelist || p == Phase.Public, "not sale phase");

        uint256 amount = msg.value;
        require(amount >= 0.01 ether, "must >= 0.01 BNB");

        if (p == Phase.Whitelist) {
            require(s.wlMap.contains(msg.sender), "not whitelisted");
            uint256 spent = s.wlMap.get(msg.sender);
            require(spent + amount <= 0.1 ether, "WL cap exceeded");
            s.wlMap.set(msg.sender, spent + amount);
        }

        uint256 secondsBought = (amount * s.durationSec) / s.targetBNB;
        require(secondsBought > 0, "too little value (seconds=0)");
        require(s.nextAvailableSecond + secondsBought <= s.durationSec, "sold out / exceed duration");

        uint256 tokenAmount = secondsBought * TOKENS_PER_SECOND * (10 ** uint256(TOKEN_DECIMALS)) / DURATION_ENLARGE;

        uint256 startSec = s.nextAvailableSecond;
        uint256 subId = s.nextSubId++;
        s.subs[subId] = Subscription({
            id: subId,
            user: msg.sender,
            amountBNB: amount,
            startSec: startSec,
            secondsBought: secondsBought,
            tokenAmount: tokenAmount,
            timestamp: block.timestamp
        });

        // accumulate contribution
        uint256 old = s.contribution.contains(msg.sender) ? s.contribution.get(msg.sender) : 0;
        s.contribution.set(msg.sender, old + amount);

        s.raisedBNB += amount;
        s.subscribedTokens += tokenAmount;
        s.nextAvailableSecond += secondsBought;

        if(s.raisedBNB > 0 && s.raisedBNB >= s.targetBNB){
            _finalize(songId);
        }

        IMusicFactory(musicFactory).emitSubscribed(address(this),s.tokenId, subId, msg.sender, amount, tokenAmount, secondsBought, startSec,songId,block.timestamp);
    }

    // Finalize
    function finalize(uint256 songId) public  nonReentrant {
        _finalize(songId);
    }

    function _finalize(uint256 songId) internal  {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");
        require(!s.finalized, "already finalized");
        Phase p = currentPhase(songId);
        require(p == Phase.Finalized || s.raisedBNB >= s.targetBNB, "cannot finalize yet");
        require(s.memeToken != address(0), "memeToken not set");
        require(fractionalVault != address(0), "fractionalVault not set");
        require(lpManager != address(0), "lpManager not set");
        require(s.raisedBNB > 0, "no contributions");
        uint256 subscribed = s.subscribedTokens;
        uint256 burnAmount = s.totalMemeSupply - subscribed;
        if (burnAmount > 0) {
            MemeToken(s.memeToken).burn(address(this), burnAmount);
        }

        IWBNB(WBNB).deposit{value: s.raisedBNB}();

        IERC20(s.memeToken).approve(lpManager, 0);
        IERC20(s.memeToken).approve(lpManager, subscribed);
        IWBNB(WBNB).approve(lpManager, 0);
        IWBNB(WBNB).approve(lpManager, s.raisedBNB);

        address token0 = s.baseToken < s.memeToken ? s.baseToken  : s.memeToken;
        address token1 = s.baseToken  < s.memeToken ? s.memeToken : s.baseToken ;

        uint256 amount0Desired = token0 == s.baseToken  ? s.raisedBNB : subscribed;
        uint256 amount1Desired = token1 == s.baseToken  ? s.raisedBNB : subscribed;

        (uint256 lpTokenId, uint128 liquidity) = ILPManagerV3(lpManager).addLiquidityAndFinalizeV3(
            songId,
            token0,
            token1,
            s.feeTier,
            s.tickLower,
            s.tickUpper,
            amount0Desired,
            amount1Desired,
            amount0Desired,
            amount1Desired,
            artist
        );

        s.lpTokenId = lpTokenId;
        s.lpLiquidity = liquidity;
        s.finalized = true;

        IMusicFactory(musicFactory).emitFinalized(address(this),s.tokenId, lpTokenId, liquidity,songId);
    }
    function getContributions(uint256 songId)
        external
        view
        returns (address[] memory users, uint256[] memory amounts)
    {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");
        uint256 n = s.contribution.length();
        users = new address[](n);
        amounts = new uint256[](n);

        for (uint256 i = 0; i < n; i++) {
            (address u, uint256 amt) = s.contribution.at(i);
            users[i] = u;
            amounts[i] = amt;
        }
    }



    function getSubscriptionCount(uint256 songId) external view returns (uint256) {
        Song storage s = songs[songId];
        require(_ownerOf(s.tokenId) != address(0), "song not exist");
        return songs[songId].nextSubId > 0 ? songs[songId].nextSubId - 1 : 0;
    }

    // helpers
    function tokenURI(uint256 tokenId) public view override returns (string memory) {
        require(_ownerOf(tokenId) != address(0), "nonexistent token");
        return string(abi.encodePacked(songs[tokenId].ipfsCid));
    }

    // admin setters 
    function setLPManager(address _lpManager) external onlyRole(UPGRADER_ROLE) { lpManager = _lpManager; }
    function setFractionalVault(address _vault) external onlyRole(UPGRADER_ROLE) {fractionalVault = _vault; }

    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721Upgradeable, AccessControlUpgradeable)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }

    receive() external payable {}
}
