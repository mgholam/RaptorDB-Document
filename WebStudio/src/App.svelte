<script>
  import { tick, onMount } from "svelte";
  import NavPanel from "./UI/nav.svelte";
  import HelpPage from "./pages/help.svelte";
  import QueryPage from "./pages/query.svelte";
  import SysInfo from "./pages/sysinfo.svelte";
  import DocSearch from "./pages/docsearch.svelte";
  import DocView from "./pages/docview.svelte";
  import DocHistory from "./pages/dochistory.svelte";
  import HFBrowser from "./pages/hfbrowser.svelte";
  import Schema from "./pages/schema.svelte";
  import SysConfig from "./pages/sysconfig.svelte";
  import Modal from "./UI/Modal.svelte";
  import Button from "./UI/Button.svelte";

  let activetabid = "";
  let tabs = [
    // {
    //   title: "query",
    //   id: "aa",
    //   obj: null,
    //   active : false,
    // },
  ];
  let showmsg = false;
  let title = "";
  let modalyes = false;

  onMount(() => {
    addtab({ name: "help", description: "Help" });
    tick().then(() => changetab(tabs[0].id));
  });

  function createcomponent(tabname, id, args) {
    var tab = null;
    switch (tabname) {
      case "help":
        tab = new HelpPage({
          target: document.getElementById(id)
        });
        break;
      case "query":
        tab = new QueryPage({
          target: document.getElementById(id)
        });
        tab.$on("showdoc", event => {
          // console.log("here : " + event.detail);
          addtab({ name: "docview", description: "View" }, event.detail);
        });
        break;
      case "sysinfo":
        tab = new SysInfo({
          target: document.getElementById(id)
        });
        break;
      case "docsearch":
        tab = new DocSearch({
          target: document.getElementById(id)
        });
        break;
      case "docview":
        tab = new DocView({
          target: document.getElementById(id),
          props: { docid: args ? args : null }
        });
        tab.$on("showrevs", event => {
          // console.log("here : " + event.detail);
          addtab({ name: "dochistory", description: "History" }, event.detail);
        });
        break;
      case "dochistory":
        tab = new DocHistory({
          target: document.getElementById(id),
          props: { docid: args ? args : null }
        });
        break;
      case "hfbrowser":
        tab = new HFBrowser({
          target: document.getElementById(id)
        });
        break;
      case "schema":
        tab = new Schema({
          target: document.getElementById(id)
        });
        break;
      case "sysconfig":
        tab = new SysConfig({
          target: document.getElementById(id)
        });
        break;
    }

    return tab;
  }

  function addtab(nav, args) {
    var e = document.getElementById("mainid");
    // create div
    var divtest = document.createElement("div");
    var id = "id" + Math.floor(Math.random() * 1000);
    divtest.id = id;
    divtest.className = "container active";
    e.appendChild(divtest);

    activetabid = id;
    tick().then(() => {
      // create svelte component and bind to dom
      var tab = createcomponent(nav.name, id, args);
      // add to list
      tabs = [
        ...tabs,
        { title: nav.description, id: id, active: true, obj: tab }
      ];
      // console.log(tabs);
      changetab(id);
    });
  }

  function removetab(id) {
    // console.log(tabs);
    var t = tabs.find(x => x.id === id);
    if (t !== null && t.obj !== null) {
      // remove svelte component
      t.obj.$destroy();
      // remove from list
      tabs = tabs.filter(x => x.id !== id);
      // remove div from dom
      var e = document.querySelector("#mainid #" + id);
      // console.log(e);
      e.remove();
    }
  }

  function showtab(nav) {
    // create content component
    // create new tab
    // bind content to tab
    addtab(nav);
  }

  function changetab(id) {
    activetabid = id;
    tabs.forEach(x => {
      x.active = x.id == id ? true : false;
      x.obj.active = x.active;
    });
    tabs = tabs;
  }

  function closetab(id) {
    //tick().then(() => {
    var i = tabs.findIndex(x => x.id === id);
    // console.log(i);
    // console.log(id);
    i--;
    changetab(tabs[i].id);
    removetab(id);
    //});
  }

  function closealltabs() {
    showmsg = false;

    tabs.forEach(x => {
      // console.log(x);
      if (x.title !== "Help") closetab(x.id);
    });

    // console.log("close all");
  }

  function closeall() {
    showmsg = true;
    modalyes = false;

    if (tabs.length == 1) title = "No Tabs to close";
    else {
      title = "Do you want to close all the tabs?";
      modalyes = true;
    }
  }
</script>

<style>
  .Container {
    width: 100%;
    backface-visibility: hidden;
    will-change: overflow;
  }

  .Left,
  .Middle
  /* .Right  */ {
    overflow: auto;
    height: auto;
    -webkit-overflow-scrolling: touch;
    -ms-overflow-style: none;
  }

  .Left::-webkit-scrollbar,
  .Middle::-webkit-scrollbar
  /* .Right::-webkit-scrollbar  */ {
    display: none;
  }

  .Left {
    width: 220px;
    float: left;
  }

  .tab-links:after {
    display: block;
    clear: both;
    content: "";
  }

  .tab-links li {
    margin: 0px 1px;
    float: left;
    list-style: none;
  }

  .tab-links {
    -webkit-padding-start: 5px;
    border-radius: 5px 5px 0px 0px;
    margin-bottom: 0px;
    margin-top: 10px;
  }

  .tab-links div {
    padding: 9px 15px;
    display: inline-block;
    border-radius: 5px 5px 0px 0px;
    background: #333333;
    font-size: 16px;
    font-weight: 600;
    color: #999999;
    transition: all linear 0.15s;
    cursor: pointer;
  }

  .tab-links div #close {
    margin-left: 10px;
    padding: 2px;
  }

  .tab-links div #close:hover {
    background: red;
    color: white;
    text-align: center;
    -webkit-border-radius: 15px;
    -moz-border-radius: 15px;
    border-radius: 15px;
    -moz-box-shadow: 1px 1px 3px #000;
    -webkit-box-shadow: 1px 1px 3px #000;
    box-shadow: 1px 1px 3px #000;
  }

  .tab-links div:hover {
    background: white;
    text-decoration: none;
  }

  li.active div,
  li.active {
    background: white;
    border-radius: 5px 5px 0px 0px;
    border: 1px;
    color: #333;
  }
  .closeall {
    margin-left: -35px;
    color: white;
    padding: 8px;
    float: right !important;
    font-weight: bolder;
    /* align-content:  center; */
    text-align: center;
    /* -webkit-border-radius: 25px;
    -moz-border-radius: 25px;
    border-radius: 25px; */
  }
  .closeall:hover {
    /* padding-right: 8px; */
    /* padding-top: 8px; */
    background: red;
    -webkit-border-radius: 15px;
    -moz-border-radius: 15px;
    border-radius: 15px;
    /* -moz-box-shadow: 1px 1px 3px #000;
    -webkit-box-shadow: 1px 1px 3px #000;
    box-shadow: 1px 1px 3px #000; */
  }
</style>

<div class="Container">
  <div class="Left">
    <NavPanel on:navclick={event => showtab(event.detail)} />
  </div>

  <div class="Middle">
    <ul class="tab-links">
      <span class="closeall" on:click={closeall} title="Close all tabs">X</span>
      {#each tabs as tab (tab.id)}
        <li
          id={tab.id}
          class:active={tab.id === activetabid}
          on:click={() => changetab(tab.id)}>
          <div>
            <label>{tab.title}</label>
            {#if tab.title !== 'Help'}
              <label id="close" on:click={() => closetab(tab.id)}>X</label>
            {/if}
          </div>
        </li>
      {/each}
    </ul>

    <div id="mainid" />

  </div>
</div>
{#if showmsg}
  <Modal on:cancel={() => (showmsg = false)} {title}>
    <div slot="footer">
      {#if modalyes}
        <Button type="button" on:click={() => closealltabs()}>Yes</Button>
      {/if}
      <Button type="button" mode="outline" on:click={() => (showmsg = false)}>
        Cancel
      </Button>
    </div>
  </Modal>
{/if}
