<template>
  <div id="copyModal" class="modal-overlay" :class="{ active: show }">
    <div class="modal-dialog">
      <div class="modal-header">
        <div class="modal-title">{{ currentServerName }}</div>
        <button class="modal-close" @click="$emit('close')">✕</button>
      </div>

      <div class="form-group">
        <label class="form-label">{{ trans.targetOs }}</label>
        <select :value="targetOs" class="form-select" @change="$emit('update:target-os', $event.target.value)">
          <option value="linux">Linux (Ubuntu/Debian/CentOS)</option>
          <option value="alpine">Alpine Linux</option>
          <option value="openwrt">OpenWrt / LEDE / ImmortalWrt</option>
          <option value="mac">macOS (Intel / Apple Silicon)</option>
          <option value="windows">Windows</option>
        </select>
      </div>

      <div class="form-row">
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.customCt }}</label>
          <input type="text" name="custom_ct" autocomplete="off" :value="customCt" class="form-input" placeholder="gd-ct-dualstack.ip.zstaticcdn.com" readonly>
        </div>
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.customCu }}</label>
          <input type="text" name="custom_cu" autocomplete="off" :value="customCu" class="form-input" placeholder="gd-cu-dualstack.ip.zstaticcdn.com" readonly>
        </div>
      </div>

      <div class="form-row">
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.customCm }}</label>
          <input type="text" name="custom_cm" autocomplete="off" :value="customCm" class="form-input" placeholder="gd-cm-dualstack.ip.zstaticcdn.com" readonly>
        </div>
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.customBd }}</label>
          <input type="text" name="custom_bd" autocomplete="off" :value="customBd" class="form-input" placeholder="lf3-ips.zstaticcdn.com" readonly>
        </div>
      </div>

      <div class="form-row">
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.collectInterval }}</label>
          <div class="flex items-center gap-2">
            <input type="text" readonly :value="collectInterval" class="form-input">
          </div>
        </div>
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.reportInterval }}</label>
          <div class="flex items-center gap-2">
            <input type="text" readonly :value="reportInterval" class="form-input">
          </div>
        </div>
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.trafficResetDay }}</label>
          <div class="flex items-center gap-2">
            <input type="text" readonly :value="resetDay" class="form-input">
            <button @click="$emit('open-edit-from-copy')" class="btn btn-icon btn-blue" :title="trans.edit">✏️</button>
          </div>
        </div>
      </div>

      <div class="form-row">
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.rxCorrection }} (GB)</label>
          <input type="number" name="rx_correction" autocomplete="off" :value="rxCorrection" class="form-input" placeholder="0" min="0" step="1" readonly>
        </div>
        <div class="form-group flex-1">
          <label class="form-label">{{ trans.txCorrection }} (GB)</label>
          <input type="number" name="tx_correction" autocomplete="off" :value="txCorrection" class="form-input" placeholder="0" min="0" step="1" readonly>
        </div>
      </div>

      <div class="form-group">
        <label class="form-label">{{ trans.installCommand }}</label>
        <div class="cmd-input-wrapper" :class="{ copied: copiedCmd }">
          <span class="cmd-prompt">{{ targetOs === 'windows' ? 'PS' : '$' }}</span>
          <input type="text" readonly :value="installCommand" class="cmd-input flex-1">
        </div>
      </div>

      <div class="modal-footer flex-justify-between">
        <button @click="$emit('copy-cmd')" class="btn btn-primary">{{ copiedCmd ? '✅ ' + trans.copied : '📋 ' + trans.copy }}</button>
        <button @click="$emit('close')" class="btn">{{ trans.cancel }}</button>
      </div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  trans: { type: Object, required: true },
  show: { type: Boolean, default: false },
  currentServerName: { type: String, default: '' },
  targetOs: { type: String, default: 'linux' },
  collectInterval: { type: [Number, String], default: 0 },
  reportInterval: { type: [Number, String], default: 60 },
  customCt: { type: String, default: '' },
  customCu: { type: String, default: '' },
  customCm: { type: String, default: '' },
  customBd: { type: String, default: '' },
  resetDay: { type: [Number, String], default: 1 },
  rxCorrection: { type: [Number, String], default: '' },
  txCorrection: { type: [Number, String], default: '' },
  installCommand: { type: String, default: '' },
  copiedCmd: { type: Boolean, default: false }
})

defineEmits(['close', 'copy-cmd', 'open-edit-from-copy', 'update:target-os'])
</script>
